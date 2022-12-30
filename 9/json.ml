open Yojson.Safe.Util
open Protocol

let id_to_json ({ id } : id) : Yojson.Safe.t =
  `Assoc [ ("id", `Int (Job_id.to_int id)) ]

let id_of_json json = { id = member "id" json |> to_int |> Job_id.of_int }
let unit_to_json () = `Assoc []

let get_response_to_json ({ id; job; pri; queue } : get_response) :
    Yojson.Safe.t =
  `Assoc
    [
      ("id", `Int (Job_id.to_int id));
      ("job", job);
      ("pri", `Int pri);
      ("queue", `String queue);
    ]

let put_of_json json =
  {
    job = member "job" json;
    pri = member "pri" json |> to_int;
    queue = member "queue" json |> to_string;
  }

let get_of_json json =
  {
    queues = member "queues" json |> to_list |> List.map to_string;
    wait =
      (try member "wait" json |> to_bool
       with Yojson.Safe.Util.Type_error _ -> false);
  }

let request_of_json : Yojson.Safe.t -> any_request =
 fun json ->
  match member "request" json |> to_string with
  | "put" -> U (Put (put_of_json json))
  | "get" -> U (Get (get_of_json json))
  | "delete" -> U (Delete (id_of_json json))
  | "abort" -> U (Abort (id_of_json json))
  | _ -> raise (Yojson.Json_error "request member")

let response_to_json body_to_json = function
  | Ok i ->
      Yojson.Safe.Util.combine (body_to_json i)
        (`Assoc [ ("status", `String "ok") ])
  | NoJob -> `Assoc [ ("status", `String "no-job") ]
  | Error msg -> `Assoc [ ("status", `String "error"); ("error", `String msg) ]

let serializer_of_request : type a. a request -> a -> Yojson.Safe.t = function
  | Put _ -> id_to_json
  | Get _ -> get_response_to_json
  | Delete _ -> unit_to_json
  | Abort _ -> unit_to_json
