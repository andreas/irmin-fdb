open Lwt.Infix

module IO = struct
  type +'a t = 'a Lwt.t

  type 'a u = 'a Lwt.t * 'a Lwt.u

  type notification = int

  let read = fst

  let fill (_, u) = Lwt.wakeup_later u

  let create = Lwt.wait

  let bind t ~f = Lwt.bind t f

  let map t ~f = Lwt.map f t

  let return = Lwt.return

  let make_notification f =
    Lwt_unix.make_notification ~once:true f

  let send_notification = Lwt_unix.send_notification
end

let store =
  Irmin_test.store (module Irmin_fdb.Make (IO)) (module Irmin.Metadata.None)

let config = Irmin_fdb.config ()

module F = Fdb.Make (IO)

let clean () =
  let open F.Infix in
  begin
    F.open_database () >>=? fun db ->
    F.Database.clear_range db ~start:"\x01" ~stop:"\xff"
  end >|= function
  | Ok () -> ()
  | Error err -> failwith (Fdb.Error.to_string err)

let init () = Lwt.return_unit
let stats = None
let suite = { Irmin_test.name = "FDB"; init; clean; config; store; stats }
