val unit_to_json : unit -> Yojson.Safe.t
val request_of_json : Yojson.Safe.t -> Protocol.any_request

val response_to_json :
  ('a -> Yojson.Safe.t) -> 'a Protocol.status -> Yojson.Safe.t

val serializer_of_request : 'a Protocol.request -> 'a -> Yojson.Safe.t
