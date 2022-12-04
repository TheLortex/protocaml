open Eio

module Session : sig
  type t

  val of_string : string -> t
  val to_string : t -> string
end = struct
  type t = int

  let of_string = int_of_string
  let to_string = string_of_int
end

module Frame = struct
  type t =
    | Connect of Session.t
    | Data of (Session.t * int * Cstruct.t)
    | Ack of (Session.t * int)
    | Close of Session.t

  let take_next_part =
    let open Eio.Buf_read.Syntax in
    let open Eio.Buf_read in
    let* v = take_while1 (( <> ) '/') in
    let+ () = char '/' in
    v

  let take_message =
    let open Eio.Buf_read.Syntax in
    let open Eio.Buf_read in
    let+ message = take_all in
    let open Astring in
    let len = String.length message in
    if message.[len - 1] = '/' then Cstruct.of_string ~len:(len - 1) message
    else failwith "parse error"

  let parser =
    let open Eio.Buf_read.Syntax in
    let open Eio.Buf_read in
    let* () = char '/' in
    let* typ' = take_next_part in
    let* session = take_next_part in
    let session = Session.of_string session in
    match typ' with
    | "connect" -> return (Connect session)
    | "close" -> return (Close session)
    | "data" ->
        let* pos = take_next_part in
        let pos = int_of_string pos in
        let* message = take_message in
        return (Data (session, pos, message))
    | "ack" ->
        let* len = take_next_part in
        let len = int_of_string len in
        return (Ack (session, len))
    | _ -> failwith "unknown message type"

  let parser_opt b = try Some (parser b) with Failure _ -> None

  let type_to_string = function
    | Connect _ -> "connect"
    | Close _ -> "close"
    | Data _ -> "data"
    | Ack _ -> "ack"

  let session = function
    | Connect session | Close session | Data (session, _, _) | Ack (session, _)
      ->
        session

  let serializer b pkt =
    let open Eio.Buf_write in
    char b '/';
    string b (type_to_string pkt);
    char b '/';
    string b (Session.to_string (session pkt));
    char b '/';
    match pkt with
    | Connect _ | Close _ -> ()
    | Data (_, pos, msg) ->
        string b (string_of_int pos);
        char b '/';
        cstruct b msg;
        char b '/'
    | Ack (_, len) ->
        string b (string_of_int len);
        char b '/'
end

module Encoding = struct
  let unescape message =
    let len = Cstruct.length message in
    let pos = ref 0 in
    let esc = ref false in
    for i = 0 to len - 1 do
      match (!esc, Cstruct.get message i) with
      | true, (('\\' | '/') as c) ->
          Cstruct.set_char message !pos c;
          esc := false;
          incr pos
      | true, _ -> failwith "calling the police"
      | _, '\\' -> esc := true
      | _, '/' -> failwith "calling the police"
      | _, c ->
          Cstruct.set_char message !pos c;
          incr pos
    done;
    (* escape eof *)
    if !pos < len then Cstruct.sub message 0 !pos else message

  let escape_slice_map ~max_size message fn =
    let len = Cstruct.length message in
    let pos = ref 0 in
    let buf_pos = ref 0 in
    let buf = Cstruct.create_unsafe max_size in
    for i = 0 to len - 1 do
      (match Cstruct.get message i with
      | ('/' | '\\') as c ->
          Cstruct.set_char buf !buf_pos '\\';
          incr buf_pos;
          Cstruct.set_char buf !buf_pos c;
          incr buf_pos
      | c ->
          Cstruct.set_char buf !buf_pos c;
          incr buf_pos);
      if !buf_pos >= max_size - 1 then (
        fn (i + 1 - !pos) (Cstruct.sub buf 0 !buf_pos);
        pos := i + 1;
        buf_pos := 0)
    done;
    if !buf_pos > 0 then fn (len - !pos) (Cstruct.sub buf 0 !buf_pos)
end

let () =
  (* a small test *)
  let msg = Cstruct.of_string "111\\///222\\333345" in
  Encoding.escape_slice_map ~max_size:5 msg (fun len slice ->
      Printf.printf "%d %s\n" len (Cstruct.to_string slice))

let max_buf_size = 1000

module Dispatcher : sig
  type t
  (** The dispatcher is in charge of UDP socket. It receives messages and
      dispatch them to the appropriate listener (either session or new
      connection). *)

  val make : Net.datagram_socket -> t
  val send : t -> Net.Sockaddr.datagram -> Frame.t -> unit
  val recv : t -> Session.t -> Frame.t
  val connect : t -> Session.t * Net.Sockaddr.datagram
end = struct
  type t = {
    socket : Net.datagram_socket;
    listeners : (Session.t, Frame.t Stream.t) Hashtbl.t;
    default : (Session.t * Net.Sockaddr.datagram) Stream.t;
    receiving : Semaphore.t;
  }

  let make socket =
    {
      socket;
      listeners = Hashtbl.create 1;
      default = Stream.create max_int;
      receiving = Semaphore.make 1;
    }

  let send t datagram msg =
    let w = Buf_write.create max_buf_size in
    Frame.serializer w msg;
    let cs = Buf_write.serialize_to_cstruct w in
    Logs.debug (fun f -> f "<-- %s" (Cstruct.to_string cs));
    Eio.Net.send t.socket datagram cs

  let rec dispatch_packets t =
    let recv_buffer = Cstruct.create_unsafe max_buf_size in
    let datagram, len =
      Semaphore.acquire t.receiving;
      Fun.protect ~finally:(fun () -> Semaphore.release t.receiving)
      @@ fun () -> Eio.Net.recv t.socket recv_buffer
    in
    Logs.debug (fun f ->
        f "--> %s" (Cstruct.sub recv_buffer 0 len |> Cstruct.to_string));
    let r =
      try
        Buf_read.of_flow ~max_size:max_buf_size
          (Flow.cstruct_source [ Cstruct.sub recv_buffer 0 len ])
        |> Frame.parser_opt
      with End_of_file -> None
    in
    Option.iter
      (fun packet ->
        let session = Frame.session packet in
        match (Hashtbl.find_opt t.listeners session, packet) with
        | None, Frame.Connect _ -> Stream.add t.default (session, datagram)
        | Some stream, _ -> Stream.add stream packet
        | None, Frame.Ack (session, _) -> send t datagram (Frame.Close session)
        | _ -> ())
      r;
    dispatch_packets t

  let recv t target =
    let stream =
      match Hashtbl.find_opt t.listeners target with
      | None ->
          let stream = Stream.create max_int in
          Hashtbl.add t.listeners target stream;
          stream
      | Some s -> s
    in
    Fiber.first (fun () -> Stream.take stream) (fun () -> dispatch_packets t)

  let connect t =
    Fiber.first (fun () -> Stream.take t.default) (fun () -> dispatch_packets t)
end

type conn_state = {
  mutable closed : bool;
  mutable send_pos : int;
  mutable send_ack : int;
  mutable recv_pos : int;
  msg : Cstruct.t Stream.t;
  mutable extra : Cstruct.t;
}

let max_header_size = 1 + 4 + 1 + 10 + 1 + 10 + 1 + 1

let connection ~sw dispatcher (session, datagram) =
  let state =
    {
      closed = false;
      send_pos = 0;
      send_ack = 0;
      recv_pos = 0;
      msg = Stream.create max_int;
      extra = Cstruct.empty;
    }
  in
  Fiber.fork ~sw (fun () ->
      while true do
        match Dispatcher.recv dispatcher session with
        | Frame.Connect _ ->
            (* they don't know we're connected *)
            Dispatcher.send dispatcher datagram (Frame.Ack (session, 0))
        | Data (_, pos, msg) when state.recv_pos = pos -> (
            try
              let msg = Encoding.unescape msg in
              let len = Cstruct.length msg in
              state.recv_pos <- len + state.recv_pos;
              Dispatcher.send dispatcher datagram
                (Frame.Ack (session, state.recv_pos));
              Stream.add state.msg msg
            with Failure _ -> ())
        | Data _ ->
            Dispatcher.send dispatcher datagram
              (Frame.Ack (session, state.recv_pos))
        | Ack (_, len) when len > state.send_ack && len <= state.send_pos ->
            state.send_ack <- len
        | Ack (_, len) when len > state.send_pos || state.closed ->
            state.closed <- true;
            Dispatcher.send dispatcher datagram (Frame.Close session)
        | Ack _ -> ()
        | Close _ ->
            state.closed <- true;
            Dispatcher.send dispatcher datagram (Frame.Close session)
      done);
  (object (self)
     inherit Flow.sink
     inherit Flow.source
     method close = ()

     method copy src =
       let msg = Cstruct.create_unsafe max_buf_size in
       try
         while true do
           let len = src#read_into msg in
           Encoding.escape_slice_map
             ~max_size:(max_buf_size - max_header_size)
             (Cstruct.sub msg 0 len)
           @@ fun buf_len buf ->
           Logs.debug (fun f -> f "<== %s" (Cstruct.to_string buf));
           let pos = state.send_pos in
           let target = state.send_pos + buf_len in
           state.send_pos <- target;
           let message = Frame.Data (session, pos, buf) in
           Dispatcher.send dispatcher datagram message;
           (* copy buffer before queuing for retransmit *)
           let buf = Cstruct.append buf Cstruct.empty in
           Fiber.fork ~sw (fun () ->
               Eio_unix.sleep 3.0;
               while state.send_ack < target && not state.closed do
                 Logs.debug (fun f -> f "<-- [RETRANSMISSION]");
                 Dispatcher.send dispatcher datagram
                   (Frame.Data (session, pos, buf));
                 Eio_unix.sleep 3.0
               done)
         done
       with End_of_file -> ()

     method read_into cstruct =
       let l0 = Cstruct.length cstruct in
       let buf =
         if Cstruct.length state.extra > 0 then (
           let buf = state.extra in
           state.extra <- Cstruct.empty;
           buf)
         else Stream.take state.msg
       in
       if Cstruct.length buf == 0 then self#read_into cstruct
       else
         let l1 = Cstruct.length buf in
         let len =
           (* target buffer can contain everything *)
           if l0 >= l1 then (
             Cstruct.blit buf 0 cstruct 0 l1;
             l1)
           else (
             (* l0 < l1: we have to split the buffer and save the extra data *)
             Cstruct.blit buf 0 cstruct 0 l0;
             state.extra <- Cstruct.sub buf l0 (l1 - l0);
             l0)
         in
         Logs.debug (fun f ->
             f "%d/%d ==> %s" l0 l1
               (Cstruct.to_string (Cstruct.sub cstruct 0 len)));
         len

     method shutdown cmd = ()
   end
    :> < Net.stream_socket ; Flow.close >)

let listen socket =
  object (self)
    inherit Net.listening_socket
    val dispatcher = Dispatcher.make socket

    method accept ~sw =
      let session, datagram = Dispatcher.connect dispatcher in
      Dispatcher.send dispatcher datagram (Frame.Ack (session, 0));
      let c = connection ~sw dispatcher (session, datagram) in
      Switch.on_release sw (fun () -> c#close);
      (c, `Unix "")

    method close = ()
  end
