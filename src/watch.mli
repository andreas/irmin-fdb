module type IO = Fdb.IO with type 'a t = 'a Lwt.t

module Make(K : Irmin.Type.S)(V : Irmin.Type.S)(IO : IO) : sig
  type t
  type watch
  type key = K.t
  type value = V.t

  val v : db:Fdb.database -> prefix:string -> t Lwt.t
  val notify: t -> Fdb.transaction -> K.t -> string option -> unit
  val watch_key: t -> K.t -> ?init:value -> (value Irmin.Diff.t -> unit Lwt.t) -> watch Lwt.t
  val watch: t -> ?init:(key * value) list ->
    (key -> value Irmin.Diff.t -> unit Lwt.t) -> watch Lwt.t
  val unwatch: t -> watch -> unit Lwt.t
end
