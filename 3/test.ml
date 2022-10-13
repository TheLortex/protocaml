
open Eio

(* 3: Budget Chat

Modern messaging software uses too many computing resources, so we're going back to basics. Budget Chat is a simple TCP-based chat room protocol.

Each message is a single line of ASCII text terminated by a newline character ('\n', or ASCII 10). Clients can send multiple messages per connection. Servers may optionally strip trailing whitespace, such as carriage return characters ('\r', or ASCII 13). All messages are raw ASCII text, not wrapped up in JSON or any other format.
*)

module Name : sig 
(* Abstract names with an enforced policy *)

  type t

  val of_string_exn : string -> t
  (* checks that the name is alphanumerical, between 1 and 32 characters. 
     an exception is raised if it doesn't respect the rules *)

  val to_string : t -> string

  val compare : t -> t -> int

  val pp : t Fmt.t
end = struct
  type t = string

  let of_string_exn name =
    let open Astring in
    let len = String.length name in
    if not
      (String.for_all Char.Ascii.is_alphanum name 
      && len > 0
      && len < 32)
    then 
      raise (Invalid_argument "name is wrong")
    else 
      name

  let to_string name = name

  let compare = String.compare

  let pp = Fmt.string

end

module Room : sig 
(* state for the chat room *)

  type t

  val v : unit -> t
  (* create a new chat room*)

  type handle

  val add : t -> Name.t -> handle
  (* register a new user and obtain a handle *)

  val read : t -> handle -> string
  (* check if the corresponding user has messages. blocks if it doesn't *)

  val write : t -> handle -> string -> unit
  (* write a message to user users *)

  val remove : t -> handle -> unit
  (* remove user from the room *)
  
end = struct
  
  module UserMap = Map.Make(Int)

  type t = {
    mutable users: (string Eio.Stream.t * Name.t) UserMap.t;
    mutable index: int;
  }

  let v () = {
    users = UserMap.empty; 
    index = 0
  }

  let broadcast t ?except message =
    UserMap.iter 
      (fun _ (stream, name) -> 
        if Some name <> except then 
          Eio.Stream.add stream message)
      t.users

  let users t = 
    UserMap.bindings t.users |> List.map (fun (_, (_, n)) -> n)
  
  type handle = int

  let add t name =
    let handle = t.index in
    t.index <- t.index + 1;
    broadcast t (Fmt.str "* %a has entered the room\n" Name.pp name);
    let stream = Eio.Stream.create max_int in
    let users_str = users t |> List.map Name.to_string |> String.concat ", " in
    Eio.Stream.add stream (Fmt.str "* The room contains: %s\n" users_str);
    t.users <- UserMap.add handle (stream, name) t.users;
    handle

  let remove t handle =
    let (_, name) = UserMap.find handle t.users in
    broadcast t (Fmt.str "* %a has left the room\n" Name.pp name);
    t.users <- UserMap.remove handle t.users

  let read t handle = 
    let (s, _) = UserMap.find handle t.users in
    Eio.Stream.take s

  let write t handle msg =
    let (_, name) = UserMap.find handle t.users in
    let msg_fmt = Fmt.str "[%a] %s\n" Name.pp name msg in
    broadcast t ~except:name msg_fmt

  
end

(* for each user, we ask who they are, then the name is registered in the room 
   and the handle is used to send and receive messages. this Room abstraction 
   enables separating the networking from the business logic. *)
let handler ~room flow _ =
  let reader = Buf_read.of_flow ~max_size:1_000_000 flow in
  Flow.copy_string "Hey, who are you?\n" flow;
  let name = Buf_read.line reader |> Name.of_string_exn in
  let handle = Room.add room name in
  try
    (* two concurrent tasks: *)
    Fiber.first
      (* - reading messages that the user would like to send *)
      (fun () -> 
        while true do
          let s = Buf_read.line reader in
          Room.write room handle s
        done)
      (* - sending messages that the user has received *)
      (fun () -> 
        while true do
          let s = Room.read room handle in
          Eio.Flow.copy_string s flow
        done)
      
  with
  | End_of_file ->
    Room.remove room handle

let () =
  Eio_main.run @@ fun env ->
  Switch.run @@ fun sw ->
  let net = Stdenv.net env in
  let socket = Net.listen 
    ~reuse_addr:true ~backlog:10 ~sw net 
    (`Tcp (Net.Ipaddr.V6.any, 10000)) 
  in
  let room = Room.v () in
  while true do
    Net.accept_fork ~sw socket ~on_error:ignore (handler ~room)
  done
