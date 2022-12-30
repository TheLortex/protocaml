val put : name:string -> Yojson.Safe.t -> int -> Job_id.t
val put_back : Protocol.get_response -> unit
val delete : Job_id.t -> bool
val get_opt : string list -> Protocol.get_response option
val get_wait : string list -> Protocol.get_response
val is_pending : Job_id.t -> bool