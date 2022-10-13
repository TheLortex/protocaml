
open Eio

(* 2: Means to an End

Your friendly neighbourhood investment bank is having trouble analysing historical price data. They need you to build a TCP server that will let clients insert and query timestamped prices.
Overview

Clients will connect to your server using TCP. Each client tracks the price of a different asset. Clients send messages to the server that either insert or query the prices.

Each connection from a client is a separate session. Each session's data represents a different asset, so each session can only query the data supplied by itself.
*)

module Database : sig
(* this module wraps the database implementation *)
  
  type t
  (* the type for a database state *)

  val v : unit -> t
  (* create a new database *)
  
  val insert : t -> timestamp:int -> int -> t
  (* insert into the database *)

  val query : t -> min:int -> max:int -> int
  (* query the mean value from the database*)

end = struct
  
  module M = Map.Make(Int)

  type t = int M.t

  let v () = M.empty

  let insert t ~timestamp v = M.add timestamp v t

  let query t ~min ~max =
    let sum = ref 0 in
    let count = ref 0 in
    M.iter 
      (fun k v ->
        if k >= min && k <= max then
          (sum := !sum + v;
           count := !count + 1)) t;
    if !count > 0 then
      !sum / !count
    else
      0

end

(* the kind of messages that we can receive *)
type message = 
  | Insert of { timestamp: int; price: int } 
  | Query of { mintime: int; maxtime: int }

(* the state transition depending on the message *)
let handle_message db = function
  | Insert { timestamp; price } -> 
    Database.insert db ~timestamp price, None
  | Query { mintime; maxtime } ->
    db, Some (Database.query db ~min:mintime ~max:maxtime)

(* a parser for messages *)
let message_reader =
  let open Buf_read in
  let open Buf_read.Syntax in
  let+ ((c, n1), n2) = any_char <*> take 4 <*> take 4 in
  let n1 = String.get_int32_be n1 0 |> Int32.to_int in
  let n2 = String.get_int32_be n2 0 |> Int32.to_int in
  match c with
  | 'Q' -> Query { mintime = n1; maxtime = n2 }
  | 'I' -> Insert { timestamp = n1; price = n2 }
  | _ -> failwith "parse error"

(* this loops over requests, keeping the state of the db around, closes the 
   connection when something fails *)
let handler flow _ =
  let buffered_reader = Buf_read.of_flow ~max_size:1_000_000 flow in
  let rec loop db =
    match message_reader buffered_reader |> handle_message db with
    | db, None -> loop db
    | db, Some mean ->
      let message = Bytes.create 4 in
      Bytes.set_int32_be message 0 (Int32.of_int mean);
      Flow.copy_string (Bytes.to_string message) flow;
      loop db
    | exception End_of_file
    | exception Failure _ -> ()
  in
  loop (Database.v ())

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
