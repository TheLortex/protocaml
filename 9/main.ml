open Eio
open Protocol

type state = (Job_id.t, get_response) Hashtbl.t

let handle_next_request : type a. state -> a request -> a status =
 fun pending -> function
  | Put { queue; job; pri } ->
      let id = Q.put ~name:queue job pri in
      Ok { id }
  | Get { queues; wait = false } -> (
      match Q.get_opt queues with
      | Some v ->
          Hashtbl.add pending v.id v;
          Ok v
      | None -> NoJob)
  | Get { queues; wait = true } ->
      let v = Q.get_wait queues in
      Hashtbl.add pending v.id v;
      Ok v
  | Delete { id } -> if Q.delete id then Ok () else NoJob
  | Abort { id } -> (
      if
        (* check that job is not deleted from another client *)
        not (Q.is_pending id)
      then NoJob
      else
        match Hashtbl.find_opt pending id with
        | None -> NoJob
        | Some v ->
            Hashtbl.remove pending v.id;
            Q.put_back v;
            Ok ())

let handler ~clock ~sw flow _ =
  Logs.info (fun f -> f "New connection!");
  let buf_read = Buf_read.of_flow ~max_size:1_000_000 flow in
  let state = Hashtbl.create 10 in
  try
    while true do
      let line = Buf_read.line buf_read in
      Logs.info (fun f -> f "<-- %s" line);
      let response =
        try
          let json = Yojson.Safe.from_string line in
          let (U request) = Json.request_of_json json in
          let response = handle_next_request state request in
          let serializer = Json.serializer_of_request request in
          Json.response_to_json serializer response
        with exn ->
          Json.response_to_json Json.unit_to_json
            (Error (Printexc.to_string exn))
      in
      let res = Yojson.Safe.to_string response in
      Logs.info (fun f -> f "--> %s" res);
      Eio.Flow.copy_string (res ^ "\n") flow
    done
  with End_of_file | Eio.Net.Connection_reset _ ->
    Hashtbl.iter
      (fun id v -> handle_next_request state (Abort { id }) |> ignore)
      state;
    Logs.info (fun f -> f "The end")

let () =
  Reporter.init ();
  Eio_linux.run ~queue_depth:2000 @@ fun env ->
  Switch.run @@ fun sw ->
  let net = Stdenv.net env in
  let clock = Stdenv.clock env in
  let socket =
    Net.listen ~backlog:10 ~reuse_addr:true ~sw net
      (`Tcp (Net.Ipaddr.V6.any, 10000))
  in
  while true do
    Net.accept_fork ~sw socket ~on_error:raise (fun flow s ->
        try handler ~clock ~sw flow s
        with Failure msg ->
          Logs.err (fun f -> f "==> %s" msg);
          Eio.Flow.shutdown flow `All)
  done
