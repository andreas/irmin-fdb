open Lwt.Infix

let src = Logs.Src.create "irmin.fdb.watch" ~doc:"Irmin FoundationDB watches"
module Log = (val Logs.src_log src : Logs.LOG)

module type IO = Fdb.IO with type 'a t = 'a Lwt.t

module Option = struct
  let bind t f = match t with None -> None | Some t -> f t
end

module List = struct
  include List

  let last t =
    List.nth t (List.length t - 1)
end

module Make(K : Irmin.Type.S)(V : Irmin.Type.S)(IO : IO) = struct
  module W = Irmin.Private.Watch.Make(K)(V)
  module F = struct
    include Fdb.Make(IO)

    exception Error of string

    let fail_if_error x =
      x >>= function
      | Ok ok -> Lwt.return ok
      | Error err -> Lwt.fail (Error (Fdb.Error.to_string err))
  end

  module KMap = Map.Make(struct type t = K.t let compare = Irmin.Type.compare K.t end)

  type t = {
    db : F.Database.t;
    data_prefix : string;
    changelog_prefix : string;
    watch_all_key : string;
    w : W.t;
    mutable keys: (int * F.Watch.t ref) KMap.t;
    mutable glob : (int * F.Watch.t ref) option;
  }

  type watch = {
    key : [`Glob | `Key of K.t];
    irmin_watch : W.watch;
  }

  type key = K.t
  type value = V.t

  let prefixed_key t k =
    let key_string = Irmin.Type.to_string K.t k in
    t.data_prefix ^ (Fdb.Tuple.pack [`Int 0; `Bytes key_string])

  let key_of_string k =
    match Irmin.Type.of_bin_string K.t k with
    | Ok v           -> v
    | Error (`Msg e) -> failwith ("Irmin_fdb.key_of_string " ^ e)

  let value_of_string v =
    match Irmin.Type.of_bin_string V.t v with
    | Ok v           -> Some v
    | Error (`Msg e) ->
      Log.err (fun l -> l "Irmin_fdb.value_of_string %s" e);
      None

  let v ~db ~prefix =
    let changelog_prefix = prefix ^ (Fdb.Tuple.pack [`Int 1]) in
    let watch_all_key = changelog_prefix in 
    F.Database.atomic_op db ~op:Fdb.Atomic_op.max ~key:watch_all_key ~param:"\000" |> F.fail_if_error >>= fun () ->
    let w = W.v () in
    let keys = KMap.empty in
    let glob = None in
    Lwt.return { db; data_prefix = prefix; changelog_prefix; watch_all_key; w; keys; glob }

  let keep_watching t key f =
    let open F.Infix in
    let rec aux watch =
      begin
        F.Watch.to_io !watch >>=? fun () ->
        F.Database.with_tx t.db ~f:(fun tx ->
          let w = F.Transaction.watch tx ~key in
          F.Transaction.get tx ~key >>|? fun value ->
          (w, value)
        )
      end
      >>= function
      | Error err ->
        Log.err (fun f -> f "Watch failed: %s" (Fdb.Error.to_string err));
        Lwt.return_unit
      | Ok (w, value) ->
        watch := w;
        f value
        >>= fun () ->
        aux watch
    in
    F.fail_if_error (F.Database.watch t.db ~key)
    >|= fun w ->
    let watch = ref w in
    Lwt.async (fun () ->
      aux watch
    );
    watch

  let watch_key t key ?init f =
    Log.debug (fun f -> f "watch_key: %a" (Irmin.Type.pp K.t) key);
    begin match KMap.find key t.keys with
    | count, fdb_watch ->
      t.keys <- KMap.add key (count+1, fdb_watch) t.keys;
      Lwt.return_unit
    | exception Not_found ->
      let k = prefixed_key t key in
      keep_watching t k (fun value ->
        let v = Option.bind value value_of_string in
        W.notify t.w key v
      ) >|= fun fdb_watch ->
      t.keys <- KMap.add key (1, fdb_watch) t.keys
    end >>= fun () ->
    W.watch_key t.w key ?init f >|= fun irmin_watch ->
    { key = `Key key; irmin_watch }

  let unwatch_key t key =
    match KMap.find key t.keys with
    | 1, fdb_watch ->
        F.Watch.cancel !fdb_watch;
        t.keys <- KMap.remove key t.keys
    | count, fdb_watch ->
        t.keys <- KMap.add key (count - 1, fdb_watch) t.keys
    | exception Not_found -> ()

  let unwatch_glob t =
    match t.glob with
    | Some (1, fdb_watch) ->
        F.Watch.cancel !fdb_watch;
        t.glob <- None
    | Some (count, fdb_watch) ->
        t.glob <- Some (count - 1, fdb_watch)
    | None -> ()

  let unwatch t watch =
    Log.debug (fun f -> f "unwatch");
    let () = match watch.key with
    | `Key k -> unwatch_key t k
    | `Glob -> unwatch_glob t
    in
    W.unwatch t.w watch.irmin_watch

  let start_fdb_watch_all t =
      let open F.Infix in
      let watch_all_end_key = Fdb.Tuple.strinc t.changelog_prefix in
      let stop = Fdb.Key_selector.first_greater_or_equal watch_all_end_key in
      let key_selector = Fdb.Key_selector.last_less_than watch_all_end_key in
      F.Database.get_key t.db ~key_selector |> F.fail_if_error
      >>= fun key ->
      let cursor = ref key in
      keep_watching t t.watch_all_key (fun _ ->
        let start = Fdb.Key_selector.first_greater_than !cursor in
        F.Database.with_tx t.db ~f:(fun tx ->
          F.Transaction.get_range tx ~start ~stop >>=? fun range_result ->
          F.Range_result.to_list range_result
        )
        |> F.fail_if_error >>= fun events ->
        cursor := List.last events |> Fdb.Key_value.key;
        events
        |> List.map Fdb.Key_value.value_bigstring
        |> List.map Fdb.Tuple.unpack_bigstring
        |> Lwt_list.iter_s (function
        | [`Int 0; `Bytes k] ->
            let key = key_of_string k in
            W.notify t.w key None
        | [`Int 1; `Bytes k; `Bytes v] ->
            let key = key_of_string k in
            let value = value_of_string v in
            W.notify t.w key value
        | _ -> assert false
        )
      )

  let fdb_watch_all t =
    match t.glob with
    | Some _ -> Lwt.return_unit
    | None ->
      start_fdb_watch_all t >|= fun fdb_watch ->
      t.glob <- Some (1, fdb_watch)

  let watch t ?init f =
    Log.debug (fun f -> f "watch");
    fdb_watch_all t >>= fun () ->
    W.watch t.w ?init f >|= fun irmin_watch ->
    { key = `Glob; irmin_watch }

  let notify t tx key value =
    Log.debug (fun f -> f "notify: %a" (Irmin.Type.pp K.t) key);
    F.Transaction.atomic_op tx ~key:t.watch_all_key ~op:Fdb.Atomic_op.add ~param:"\001";
    let k = Irmin.Type.to_bin_string K.t key in
    let param = match value with
    | None ->
        Fdb.Tuple.pack [`Int 0; `Bytes k]
    | Some v ->
        Fdb.Tuple.pack [`Int 1; `Bytes k; `Bytes v]
    in
    let buf = Buffer.create 32 in
    Buffer.add_string buf t.changelog_prefix;
    Buffer.add_string buf "0000000000";
    Buffer.add_char buf (Char.chr (String.length t.changelog_prefix));
    Buffer.add_string buf "\000\000\000";
    let key = Buffer.contents buf in
    F.Transaction.atomic_op tx ~key ~op:Fdb.Atomic_op.set_versionstamped_key ~param
end
