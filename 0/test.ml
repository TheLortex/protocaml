
open Eio

(* 0: Smoke Test 
   
Deep inside Initrode Global's enterprise management framework lies a component 
that writes data to a server and expects to read the same data back. (Think of 
it as a kind of distributed system delay-line memory). We need you to write the 
server to echo the data back.

Accept TCP connections.

Whenever you receive data from a client, send it back unmodified.

Make sure you don't mangle binary data, and that you can handle at least 5 
simultaneous clients.

Once the client has finished sending data to you it shuts down its sending side.
Once you've reached end-of-file on your receiving side, and sent back all the 
data you've received, close the socket so that the client knows you've finished.
(This point trips up a lot of proxy software, such as ngrok; if you're using a 
  proxy and you can't work out why you're failing the check, try hosting your 
server in the cloud instead).

Your program will implement the TCP Echo Service from RFC 862.
*)

let handler flow _ =
  (* well, at least this is straightforward. what we receive is sent back *)
  Eio.Flow.copy flow flow

let () =
  Eio_main.run @@ fun env ->
  Switch.run @@ fun sw ->
  let net = Stdenv.net env in
  let socket = Net.listen 
    ~reuse_addr:true ~backlog:10 ~sw net 
    (`Tcp (Net.Ipaddr.V6.any, 10000)) 
  in
  while true do
    Net.accept_fork ~sw socket ~on_error:raise handler
  done
