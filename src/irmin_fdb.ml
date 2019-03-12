open Lwt.Infix

let src = Logs.Src.create "irmin.fdb" ~doc:"Irmin FoundationDB store"
module Log = (val Logs.src_log src : Logs.LOG)

module type IO = Fdb.IO with type 'a t = 'a Lwt.t

module Option = struct
  let bind t f = match t with None -> None | Some t -> f t
end

module Read_only (IO : IO) (K: Irmin.Type.S) (V: Irmin.Type.S) = struct
  module F = struct
    include Fdb.Make(IO)

    exception Error of string

    let fail_if_error x =
      x >>= function
      | Ok ok -> Lwt.return ok
      | Error err -> Lwt.fail (Error (Fdb.Error.to_string err))
  end

  type key = K.t
  type value = V.t
  type 'a t = {
    db : F.Database.t;
    prefix : string;
  }
  let v prefix config =
    let open F.Infix in
    let module C = Irmin.Private.Conf in
    let prefix = match C.get config C.root with
      | Some root -> Fdb.Tuple.pack [`Bytes root; `Bytes prefix]
      | None -> Fdb.Tuple.pack [`Bytes prefix]
    in
    begin
      F.open_database () >>|? fun db ->
      { db; prefix }
    end
    |> F.fail_if_error

  let cast t = (t :> [`Read | `Write] t)
  let batch t f = f (cast t)

  let value v =
    match Irmin.Type.of_bin_string V.t v with
    | Ok v           -> Some v
    | Error (`Msg e) ->
      Log.err (fun l -> l "Irmin_fdb.value %s" e);
      None

  let prefixed_key t k =
    let key_string = Irmin.Type.to_string K.t k in
    t.prefix ^ (Fdb.Tuple.pack [`Int 0; `Bytes key_string])

  let find t key =
    let open F.Infix in
    let key = prefixed_key t key in
    Log.debug (fun f -> f "find %s" (String.escaped key));
    begin
      F.Database.get t.db ~key
      >>|? function
      | None -> None
      | Some x -> value x
    end
    |> F.fail_if_error

  let mem t key =
    let open F.Infix in
    let key = prefixed_key t key in
    Log.debug (fun f -> f "mem %s" (String.escaped key));
    let key_selector = Fdb.Key_selector.first_greater_or_equal key in
    begin
      F.Database.get_key t.db ~key_selector
      >>|? fun key' ->
      String.equal key key'
    end
    |> F.fail_if_error

end

module Append_only (IO : IO) (K: Irmin.Type.S) (V: Irmin.Type.S) = struct

  include Read_only(IO)(K)(V)

  let v config =
    v "obj" config

  let add t key value =
    let key = prefixed_key t key in
    Log.debug (fun f -> f "add -> %s" (String.escaped key));
    let value = Irmin.Type.to_bin_string V.t value in
    F.Database.set t.db ~key ~value
    |> F.fail_if_error

end

module Atomic_write (IO : IO) (K: Irmin.Type.S) (V: Irmin.Type.S) = struct

  module RO = Read_only(IO)(K)(V)
  module F = RO.F
  module W = Watch.Make(K)(V)(IO)

  type t = {
    ro: unit RO.t;
    w: W.t;
  }
  type key = RO.key
  type value = RO.value
  type watch = W.watch

  let v config =
    RO.v "data" config >>= fun ro ->
    W.v ~db:ro.db ~prefix:ro.prefix >>= fun w ->
    Lwt.return { ro; w }

  let key_without_prefix k =
    match k with
    | [`Bytes _prefix; `Int 0; `Bytes k] -> k
    | _ -> failwith ("Invalid key: " ^ (Fdb.Tuple.pack k |> String.escaped))

  let key_of_string k =
    match Irmin.Type.of_bin_string K.t k with
    | Ok v           -> v
    | Error (`Msg e) -> failwith ("Irmin_fdb.key_of_string " ^ e)

  let find t = RO.find t.ro
  let mem t = RO.mem t.ro

  let watch t = W.watch t.w
  let watch_key t = W.watch_key t.w
  let unwatch t = W.unwatch t.w

  let list t =
    let open F.Infix in
    Log.debug (fun f -> f "list");
    F.Database.with_tx t.ro.db ~f:(fun tx ->
      let prefix = Fdb.Key_selector.first_greater_than (t.ro.prefix ^ (Fdb.Tuple.pack [`Int 0])) in
      F.Transaction.get_range_prefix tx ~prefix >>=? fun range_result ->
      F.Range_result.to_list range_result
    )
    >>|? List.map (fun kv ->
      kv
      |> Fdb.Key_value.key_bigstring
      |> Fdb.Tuple.unpack_bigstring
      |> key_without_prefix
      |> key_of_string
    )
    |> F.fail_if_error

  let set t key value =
    let k = RO.prefixed_key t.ro key in
    Log.debug (fun f -> f "update %s" (String.escaped k));
    let v = Irmin.Type.to_bin_string V.t value in
    F.Database.with_tx t.ro.db ~f:(fun tx ->
      F.Transaction.set tx ~key:k ~value:v;
      W.notify t.w tx key (Some v);
      Lwt.return (Ok ())
    )
    |> F.fail_if_error

  let remove t key =
    let k = RO.prefixed_key t.ro key in
    Log.debug (fun f -> f "remove %s" (String.escaped k));
    F.Database.with_tx t.ro.db ~f:(fun tx ->
      F.Transaction.clear tx ~key:k;
      W.notify t.w tx key None;
      Lwt.return (Ok ())
    )
    |> F.fail_if_error

  let test_and_set t key ~test ~set =
    let open F.Infix in
    let k = RO.prefixed_key t.ro key in
    Log.debug (fun f -> f "test_and_set %s" (String.escaped k));
    F.Database.with_tx t.ro.db ~f:(fun tx ->
      F.Transaction.get tx ~key:k >>=? fun v ->
      let v = Option.bind v RO.value in
      if Irmin.Type.(equal (option V.t)) test v then (
        let () = match set with
          | None   ->
            F.Transaction.clear tx ~key:k;
            W.notify t.w tx key None
          | Some set ->
            let value = Irmin.Type.to_bin_string V.t set in
            F.Transaction.set tx ~key:k ~value;
            W.notify t.w tx key (Some value)
        in
        Lwt.return (Ok true)
      ) else
        Lwt.return (Ok false)
    )
    |> F.fail_if_error
end

let config () = Irmin.Private.Conf.empty

module Make (IO : IO)
    (M: Irmin.Metadata.S)
    (C: Irmin.Contents.S)
    (P: Irmin.Path.S)
    (B: Irmin.Branch.S)
    (H: Irmin.Hash.S)
= struct
  module AO = Append_only(IO)
  module AW = Atomic_write(IO)
  include Irmin.Make(Irmin_chunk.Content_addressable(AO))(AW)(M)(C)(P)(B)(H)
end

module KV (IO : IO) (C: Irmin.Contents.S) =
  Make
    (IO)
    (Irmin.Metadata.None)
    (C)
    (Irmin.Path.String_list)
    (Irmin.Branch.String)
    (Irmin.Hash.SHA1)
