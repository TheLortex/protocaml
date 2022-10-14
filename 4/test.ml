
open Eio
open Astring

(* 4: Unusual Database Program

It's your first day on the job. Your predecessor, Ken, left in mysterious circumstances, but not before coming up with a protocol for the new key-value database. You have some doubts about Ken's motivations, but there's no time for questions! Let's implement his protocol.

Ken's strange database is a key-value store accessed over UDP. Since UDP does not provide retransmission of dropped packets, and neither does Ken's protocol, clients have to be careful not to send requests too fast, and have to accept that some requests or responses may be dropped.

Each request, and each response, is a single UDP packet.

There are only two types of request: insert and retrieve. Insert allows a client to insert a value for a key, and retrieve allows a client to retrieve the value for a key.
*)

type state = (string, string) Hashtbl.t

let state = Hashtbl.create 100

let buffer = Cstruct.create_unsafe 10000

let handle socket =
  let (from, len) = Net.recv socket buffer in

  let respond response = 
    Net.send socket from (Cstruct.of_string response) 
  in

  let message = Cstruct.to_string ~len buffer in
  match String.cut ~sep:"=" message with
  | None when message = "version" -> respond "version=Protocaml v0.42"
  | None -> 
    let value = Hashtbl.find_opt state message |> Option.value ~default:"" in
    respond (message ^ "=" ^ value)
  | Some (key, value) -> Hashtbl.replace state key value

let () =
  Eio_main.run @@ fun env ->
  Switch.run @@ fun sw ->
  let net = Stdenv.net env in
  let socket = Net.datagram_socket ~sw net (`Udp (Net.Ipaddr.V6.any, 10000)) in
  while true do
    handle socket
  done
