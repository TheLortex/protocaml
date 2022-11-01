open Eio


type camera = {road: int; mile: int; limit: int}

type dispatcher = { roads: int list}

type ticket = {plate: string; road: int; mile1: int; timestamp1: int; mile2: int; timestamp2: int; speed: int}

type client_message = 
  | Plate of {plate: string; timestamp: int} 
  | WantHeartbeat of {interval: int}
  | IAmCamera of camera
  | IAmDispatcher of dispatcher

type server_message =
  | Error of {msg: string} 
  | Ticket of ticket
  | Heartbeat

module Parse = struct
  
  open Buf_read.Syntax

  let u8 = 
    let+ c = Buf_read.any_char in
    Char.code c

  let u16 =
    let+ c = Buf_read.take 2 in
    String.get_uint16_be c 0

  let u32 =
    let+ c = Buf_read.take 4 in
    String.get_int32_be c 0 |> Int32.to_int

  let list parser =
    let rec parse acc =
      function
      | 0 -> Buf_read.return (List.rev acc)
      | n -> 
        let* v = parser in
        parse (v::acc) (n-1)
    in
    let* sz = u8 in
    parse [] sz
    
  let string =
    let* sz = u8 in
    Buf_read.take sz

  let client_message =
    let open Buf_read.Syntax in
    let* msg_type = u8 in
    match msg_type with
    | 0x20 ->
      let* plate = string in
      let+ timestamp = u32 in
      Plate { plate; timestamp }
    | 0x40 -> 
      let+ interval = u32 in
      WantHeartbeat { interval }
    | 0x80 -> 
      let* road = u16 in
      let* mile = u16 in
      let+ limit = u16 in
      IAmCamera {road; mile; limit}
    | 0x81 ->
      let+ roads = (list u16) in
      IAmDispatcher { roads }
    | _ -> failwith "parse error"
  
end

module Serialize = struct
  
  open Buf_write


  let string w str =
    let len = String.length str in
    char w (Char.chr len);
    string w str

  let u8 w v = char w (Char.chr v)

  let u16 w v = BE.uint16 w v
  
  let u32 w v = BE.uint32 w (Int32.of_int v)

  let server_message w =
    function
    | Error {msg} ->
      u8 w (0x10);
      string w msg

    | Ticket {plate; road; mile1; timestamp1; mile2; timestamp2; speed} ->
      u8 w (0x21);
      string w plate;
      u16 w road;
      u16 w mile1;
      u32 w timestamp1;
      u16 w mile2;
      u32 w timestamp2;
      u16 w speed

    | Heartbeat -> 
      u8 w (0x41)
    

  let to_flow srz flow =
    Switch.run @@ fun sw ->
    let w = Buf_write.create ~sw 1000 in
    srz w;
    let msg = Buf_write.serialize_to_string w in
    Flow.copy_string msg flow
end

type plate = string

type plate_info = {
  plate: string;
  timestamp: int;
  mile: int;
  limit: int;
  road: int;
}


let dispatchers = Hashtbl.create 10

let get_or_create_dispatcher road =
  match Hashtbl.find_opt dispatchers road with
  | Some v -> v
  | None -> 
    let v = Stream.create max_int in
    Hashtbl.add dispatchers road v;
    v

let spawn_heartbeat ~v ~sw ~clock flow interval =
  if interval > 0 then
  (Logs.debug (fun f -> f "[%d] H %d" v interval);
  Eio.Fiber.fork ~sw @@ fun () ->
  let t = ref (Time.now clock) in
  while true do
    let next = !t +. (Float.of_int interval) *. 0.1 in
    let now = Time.now clock in
    (if (next > now) then
      if (next > now +. 0.01) then
        Time.sleep_until clock next
      else
        ()
    else
      Logs.err (fun f -> f "[%d] Heartbeat lag" v));
    t := next;
    Logs.debug (fun f -> f "[%d] H %d" v interval);
    Serialize.(to_flow (fun w -> server_message w Heartbeat)) flow ;
    Logs.debug (fun f -> f "[%d] HOK" v)
  done)

let error flow msg =
  Serialize.(to_flow (fun w -> server_message w (Error { msg }))) flow 

let camera ~clock ~v ~sw ~flow p ({road; mile; limit} : camera) =
  while true do 
    match Parse.client_message p with
    | WantHeartbeat {interval} -> spawn_heartbeat ~clock ~v ~sw flow interval
    | Plate { plate; timestamp } ->
      Logs.info (fun f -> f "<%d> Plate: %s %d %d" road plate timestamp mile);
      let stream = get_or_create_dispatcher road in
      Stream.add stream { limit; plate; timestamp; mile; road }
    | _ -> error flow "cam: illegal message"
  done

let dispatcher_input ~clock ~v ~sw ~flow p =
  while true do 
    match Parse.client_message p with
    | WantHeartbeat {interval} -> spawn_heartbeat ~clock ~v ~sw flow interval
    | _ -> error flow "disp: illegal message"
  done

type flash = {
  mile: int;
  timestamp: int;
  road: int;
}

type controller_state = {
  flashes: (plate, flash list) Hashtbl.t;
  last_ticket: (plate * int, unit) Hashtbl.t;
}

let day ts = ts / 86400

let is_overspeed ~already_fined (plate_info: plate_info) {mile; timestamp; road} =
  Fiber.yield ();
  if plate_info.road <> road then None
  else
  if already_fined (day plate_info.timestamp) || already_fined (day timestamp) then
    None
  else
    let distance = Int.abs (plate_info.mile - mile) in
    let timedelta = Int.abs (plate_info.timestamp - timestamp) in
    (* miles * seconds / hour > seconds * miles / hour *)
    if distance * 3600 > timedelta * plate_info.limit then
      let speed = 100 * distance * 3600 / timedelta in
      let mile1, timestamp1, mile2, timestamp2 =
        if plate_info.timestamp < timestamp then
          plate_info.mile, plate_info.timestamp, mile, timestamp
        else
          mile, timestamp, plate_info.mile, plate_info.timestamp
      in
      Logs.info (fun f -> f "<%d> Ticket: %s %d" road plate_info.plate speed);
      Some {
        plate = plate_info.plate; 
        road = plate_info.road; 
        mile1; 
        timestamp1; 
        mile2; 
        timestamp2; 
        speed
      }
    else 
      None


let find_ticket { flashes; last_ticket } plate_info = 
  match Hashtbl.find_opt flashes plate_info.plate with
  | Some o ->
    let already_fined day = 
      Hashtbl.mem last_ticket (plate_info.plate, day)
    in
    List.find_map (is_overspeed ~already_fined plate_info) o  
  | None -> None

let update controller_state plate_info =
  let ticket = find_ticket controller_state plate_info in
  let st = 
    Hashtbl.find_opt controller_state.flashes plate_info.plate
    |> Option.value ~default:[]
  in
  Hashtbl.replace 
    controller_state.flashes 
    plate_info.plate 
    ({ mile = plate_info.mile; timestamp = plate_info.timestamp; road = plate_info.road } :: st);
  (match ticket with
  | Some ticket ->
    (
      Hashtbl.replace controller_state.last_ticket (plate_info.plate, day (ticket.timestamp1)) ();
      Hashtbl.replace controller_state.last_ticket (plate_info.plate, day (ticket.timestamp2)) ())
  | None ->());
  ticket


let dispatcher_workers_state = Hashtbl.create 10 

let get_or_create_state road =
  match Hashtbl.find_opt dispatcher_workers_state road with
  | Some v -> v
  | None -> 
    let v = {flashes = Hashtbl.create 0; last_ticket = Hashtbl.create 0} in
    Hashtbl.add dispatcher_workers_state road v;
    v

let worker ~flow p road stream =  
  let controller_state = get_or_create_state road in
  while true do
    let plate = Stream.take stream in
    match update controller_state plate with
    | None -> ()
    | Some t -> 
      Serialize.(to_flow (fun w -> server_message w (Ticket t))) flow  
  done


let dispatcher_output ~flow p { roads } =
  List.map (fun i () -> worker ~flow p i (get_or_create_dispatcher i)) roads 
  |> Fiber.all

let c = ref 0 

let handler ~clock ~sw:_ _net flow _ =
  let v = !c in 
  incr c;
  Logs.info (fun f -> f "[%d] New connection" v);
  try
    Eio.Switch.run @@ fun sw ->
    let p = Buf_read.of_flow ~max_size:100000 flow in
    while true do
      match Parse.client_message p with
      | IAmCamera c -> 
        Logs.info (fun f -> f "[%d] Camera: %d %d %d" v c.road c.limit c.mile);
        camera ~clock ~v ~flow ~sw p c 
      | IAmDispatcher d -> 
        Logs.info (fun f -> f "[%d] Dispatcher: %d %a" v (List.length d.roads) Fmt.(list int) d.roads);
        Fiber.both 
          (fun () -> dispatcher_input ~clock ~v ~flow ~sw p)
          (fun () -> dispatcher_output ~flow p d)
      | WantHeartbeat { interval } -> spawn_heartbeat ~clock ~v ~sw flow interval
      | _ -> Flow.shutdown flow `All
    done
  with
  | Eio.Net.Connection_reset _ -> 
    Logs.info (fun f -> f "[%d] Connection reset" v)
  | End_of_file ->
    (Flow.shutdown flow `All;
    Logs.info (fun f -> f "[%d] EOF" v))
  | Failure msg ->
    error flow msg
  


let c = Mtime_clock.counter ()

let reporter ppf =
  let report src level ~over k msgf =
    let k _ = over (); k () in
    let with_stamp h tags k ppf fmt =
      Format.kfprintf k ppf ("%a[%a] @[" ^^ fmt ^^ "@]@.")
        Logs.pp_header (level, h) Mtime.Span.pp (Mtime_clock.count c)
    in
    msgf @@ fun ?header ?tags fmt -> with_stamp header tags k ppf fmt
  in
  { Logs.report = report }

let () =
  Logs.set_level (Some Info);
  Logs.set_reporter (reporter (Format.std_formatter));
  Eio_linux.run ~queue_depth:300 @@ fun env ->
  Switch.run @@ fun sw ->
  let net = Stdenv.net env in
  let clock = Stdenv.clock env in
  let socket = Net.listen 
    ~reuse_addr:true ~backlog:1000 ~sw net 
    (`Tcp (Net.Ipaddr.V6.any, 10000)) 
  in
  while true do
    Logs.debug (fun f -> f "W");
    Net.accept_fork ~sw socket ~on_error:raise (handler ~clock ~sw net)
  done
