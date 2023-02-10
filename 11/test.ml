open Eio

module Protocol = struct
  type range = { species : string; min : int; max : int }
  type action = Cull | Conserve
  type population = { species : string; count : int }
  type site_visit = { site : int; populations : population list }

  let pp_population f { species; count } =
    Fmt.pf f "{species=%s,count=%d}" species count

  let pp_range f { species; min; max } =
    Fmt.pf f "{species=%s,min=%d,max=%d}" species min max

  type 'a message =
    | Hello : { protocol : string; version : int } -> 'a message
    | Error : { message : string } -> [< `To_client | `To_server ] message
    | OK : [ `From_server ] message
    | DialAuthority : { site : int } -> [ `To_server ] message
    | TargetPopulations : {
        site : int;
        populations : range list;
      }
        -> [ `From_server ] message
    | CreatePolicy : {
        species : string;
        action : action;
      }
        -> [ `To_server ] message
    | DeletePolicy : { policy : int } -> [ `To_server ] message
    | PolicyResult : { policy : int } -> [ `From_server ] message
    | SiteVisit : site_visit -> [ `From_client ] message

  let to_string : type a. a message -> string = function
    | Hello { protocol; version } ->
        Fmt.str "Hello{protocol=%s;version=%d}" protocol version
    | Error { message } -> "Error: " ^ message
    | OK -> "OK"
    | DialAuthority { site } -> Fmt.str "Dial{site=%d}" site
    | TargetPopulations { site; populations } ->
        Fmt.str "TargetPopulations{site=%d,populations=%a}" site
          (Fmt.list pp_range) populations
    | CreatePolicy { species; action } ->
        Fmt.str "CreatePolicy{species=%s,action=%s}" species
          (match action with Cull -> "cull" | Conserve -> "conserve")
    | DeletePolicy { policy } -> Fmt.str "DeletePolicy{policy=%d}" policy
    | PolicyResult { policy } -> Fmt.str "PolicyResult{policy=%d}" policy
    | SiteVisit { site; populations } ->
        Fmt.str "SiteVisit{site=%d,populations=%a}" site
          (Fmt.list pp_population) populations

  module Parse = struct
    open Buf_read.Syntax

    let u8 =
      let+ c = Buf_read.any_char in
      Char.code c

    let u32 =
      let+ c = Buf_read.take 4 in
      String.get_int32_be c 0 |> Int32.to_int

    let list parser =
      let rec parse acc = function
        | 0 -> Buf_read.return (List.rev acc)
        | n ->
            let* v = parser in
            parse (v :: acc) (n - 1)
      in
      let* sz = u32 in
      parse [] sz

    let string =
      let* sz = u32 in
      Buf_read.take sz
  end

  module Ser = struct
    open Buf_write

    let string f s =
      let sz = String.length s in
      BE.uint32 f (Int32.of_int sz);
      string f s

    let u32 f u = BE.uint32 f (Int32.of_int u)
    let action f = function Cull -> uint8 f 0x90 | Conserve -> uint8 f 0xa0
  end

  let parse_range b =
    let species = Parse.string b in
    let min = Parse.u32 b in
    let max = Parse.u32 b in
    { species; min; max }

  let parse_population b =
    let species = Parse.string b in
    let count = Parse.u32 b in
    { species; count }

  let checksum_correct buffer =
    let v = ref 0 in
    for i = 0 to Cstruct.length buffer - 1 do
      v := !v + Cstruct.get_uint8 buffer i
    done;
    !v mod 256 = 0

  type 'a input =
    | From_client : int -> [ `From_client ] input
    | From_server : int -> [ `From_server ] input

  type 'a output =
    | To_client : int -> [ `To_client ] output
    | To_server : int -> [ `To_server ] output

  let get : type a. a input -> Buf_read.t -> a message =
   fun kind b ->
    Buf_read.ensure b 5;
    (* id and length *)
    let length = Cstruct.BE.get_uint32 (Buf_read.peek b) 1 |> Int32.to_int in
    if length < 5 || length >= 1000000 then failwith "invalid length";
    Buf_read.ensure b length;
    if not (checksum_correct (Cstruct.sub (Buf_read.peek b) 0 length)) then
      failwith "invalid checksum"
    else
      let id = Parse.u8 b in
      let _ = Parse.u32 b in
      let content =
        Buf_read.take (length - 4 - 1 - 1) b |> Buf_read.of_string
      in
      let _ = Parse.u8 b in
      let (msg : a message) =
        match (id, kind) with
        | 0x50, _ ->
            let protocol = Parse.string content in
            let version = Parse.u32 content in
            Hello { protocol; version }
        | 0x52, From_server _ -> OK
        | 0x54, From_server _ ->
            let site = Parse.u32 content in
            let populations = Parse.list parse_range content in
            TargetPopulations { site; populations }
        | 0x57, From_server _ ->
            let policy = Parse.u32 content in
            PolicyResult { policy }
        | 0x58, From_client _ ->
            let site = Parse.u32 content in
            let populations = Parse.list parse_population content in
            SiteVisit { site; populations }
        | _ -> failwith "unknown message code"
      in
      (match kind with
      | From_server i -> Logs.info (fun f -> f "%10d <== %s" i (to_string msg))
      | From_client i -> Logs.info (fun f -> f "%5d <-- %s" i (to_string msg)));
      if Buf_read.at_end_of_input content then msg
      else failwith "not at end of input for content"

  let compute_checksum ~id ~message ~len =
    let v = id + len + (len lsr 8) + (len lsr 16) + (len lsr 24) in
    let tot = String.fold_left (fun v c -> v + Char.code c) v message in
    (256 - (tot mod 256)) mod 256

  let send : type a. a output -> Buf_write.t -> a message -> unit =
   fun kind b msg ->
    let id, message =
      let b = Buf_write.create 128 in
      (* todo write message *)
      let id =
        match (msg, kind) with
        | Hello { protocol; version }, (To_client _ | To_server _) ->
            Ser.string b protocol;
            Ser.u32 b version;
            0x50
        | Error { message }, (To_client _ | To_server _) ->
            Ser.string b message;
            0x51
        | DialAuthority { site }, To_server _ ->
            Ser.u32 b site;
            0x53
        | CreatePolicy { action; species }, To_server _ ->
            Ser.string b species;
            Ser.action b action;
            0x55
        | DeletePolicy { policy }, To_server _ ->
            Ser.u32 b policy;
            0x56
        | OK, _ -> .
        | TargetPopulations _, _ -> .
        | PolicyResult _, _ -> .
        | SiteVisit _, _ -> .
      in
      (id, Buf_write.serialize_to_string b)
    in
    let len = String.length message + 4 + 1 + 1 in
    let checksum = compute_checksum ~id ~message ~len in
    Buf_write.uint8 b id;
    Buf_write.BE.uint32 b (Int32.of_int len);
    Buf_write.string b message;
    Buf_write.uint8 b checksum;
    Buf_write.flush b;
    match kind with
    | To_server i -> Logs.info (fun f -> f "%10d ==> %s" i (to_string msg))
    | To_client i -> Logs.info (fun f -> f "%5d --> %s" i (to_string msg))

  let send_flow a flow msg = Buf_write.with_flow flow @@ fun b -> send a b msg
end

type policy = Nothing | Cull of int | Conserve of int

type authority = {
  populations_mutex : Eio.Mutex.t;
  populations : (string, Protocol.range * policy) Hashtbl.t;
  flow : Eio.Flow.two_way;
  input : Eio.Buf_read.t;
  id : int;
}

type authority_state = (int, authority Eio.Promise.t) Hashtbl.t

let authorities : authority_state = Hashtbl.create 100

let expect_hello v i =
  match Protocol.get v i with
  | Protocol.Hello { protocol = "pestcontrol"; version = 1 } -> ()
  | Hello _ -> failwith "unexpected hello content"
  | _ -> failwith "unexpected message instead of hello"

let expect_site_visit v i =
  match Protocol.get v i with
  | Protocol.SiteVisit e -> e
  | _ -> failwith "unexpected message instead of site visit"

let expect_ok v i =
  match Protocol.get v i with
  | Protocol.OK -> ()
  | _ -> failwith "unexpected message instead of ok"

let expect_populations v i =
  match Protocol.get v i with
  | Protocol.TargetPopulations e -> e.populations
  | _ -> failwith "unexpected message instead of target populations"

let expect_policy_result v i =
  match Protocol.get v i with
  | Protocol.PolicyResult { policy } -> policy
  | _ -> failwith "unexpected message instead of policy result"

let send_hello v f =
  Protocol.send_flow v f (Hello { protocol = "pestcontrol"; version = 1 })

let get_authority ~connect site =
  try Eio.Promise.await (Hashtbl.find authorities site)
  with Not_found ->
    let promise, resolve = Eio.Promise.create () in
    Hashtbl.replace authorities site promise;
    let flow = (connect () :> Eio.Flow.two_way) in
    let input = Buf_read.of_flow ~max_size:100000 flow in
    send_hello (To_server site) flow;
    expect_hello (From_server site) input;
    Protocol.send_flow (To_server site) flow (DialAuthority { site });
    let populations =
      expect_populations (From_server site) input
      |> List.map (fun (pop : Protocol.range) -> (pop.species, (pop, Nothing)))
      |> List.to_seq |> Hashtbl.of_seq
    in
    let v =
      {
        populations;
        flow;
        input;
        id = site;
        populations_mutex = Mutex.create ();
      }
    in
    Promise.resolve resolve v;
    v

let handle_site_visit ~connect (site_visit : Protocol.site_visit) =
  let counts =
    let v = Hashtbl.create 100 in
    site_visit.populations
    |> List.map (fun (v : Protocol.population) -> (v.species, v.count))
    |> List.to_seq
    |> Seq.iter (fun (a, b) ->
           match Hashtbl.find_opt v a with
           | None -> Hashtbl.add v a b
           | Some b' when b = b' -> ()
           | _ -> failwith "inconsistent readings");
    v
  in
  let { populations; flow; input; id; populations_mutex } =
    get_authority ~connect site_visit.site
  in
  Mutex.use_rw ~protect:false populations_mutex @@ fun () ->
  Hashtbl.iter
    (fun _ ({ Protocol.min; max; species }, current_policy) ->
      let count = Hashtbl.find_opt counts species |> Option.value ~default:0 in
      let target_policy =
        if min <= count && count <= max then None
        else if count < min then Some Protocol.Conserve
        else Some Protocol.Cull
      in
      let to_delete =
        match (target_policy, current_policy) with
        | None, Nothing -> None
        | Some Protocol.Conserve, Conserve _ -> None
        | Some Protocol.Cull, Cull _ -> None
        | _, (Cull p | Conserve p) -> Some p
        | _, Nothing -> None
      in
      let to_add =
        match (target_policy, current_policy) with
        | None, Nothing -> None
        | Some Protocol.Conserve, Conserve _ -> None
        | Some Protocol.Cull, Cull _ -> None
        | Some target, _ -> Some target
        | None, _ -> None
      in
      (match to_delete with
      | Some policy ->
          Protocol.send_flow (To_server id) flow (DeletePolicy { policy });
          expect_ok (From_server id) input
      | None -> ());
      let state =
        match (to_add, target_policy, current_policy) with
        | Some target, _, _ -> (
            Protocol.send_flow (To_server id) flow
              (CreatePolicy { species; action = target });
            let p = expect_policy_result (From_server id) input in
            match target with Protocol.Cull -> Cull p | Conserve -> Conserve p)
        | None, None, _ -> Nothing
        | None, Some Protocol.Conserve, Conserve v -> Conserve v
        | None, Some Protocol.Cull, Cull v -> Cull v
        | _ -> failwith "programming error"
      in
      Hashtbl.replace populations species ({ Protocol.min; max; species }, state))
    populations

let handler ~connect flow s =
  let id = match s with `Tcp (_, port) -> port | _ -> -1 in
  let input = Buf_read.of_flow ~max_size:10_000_000 flow in
  try
    try
      send_hello (To_client id) flow;
      expect_hello (From_client id) input;
      while true do
        let site_visit = expect_site_visit (From_client id) input in
        handle_site_visit ~connect site_visit
      done
    with Failure message ->
      Protocol.send_flow (To_client id) flow (Error { message });
      Eio.Flow.shutdown flow `All
  with
  | End_of_file ->
      Logs.info (fun f -> f "EOF");
      Protocol.send_flow (To_client id) flow (Error { message = "EOF" })
  | Eio.Net.Connection_reset _ -> Logs.info (fun f -> f "%d: disconnected" id)

let () =
  Reporter.init ();
  Logs.set_level (Some Info);
  Eio_linux.run ~queue_depth:2000 @@ fun env ->
  Switch.run @@ fun sw ->
  let net = Stdenv.net env in
  let socket =
    Net.listen ~reuse_addr:true ~backlog:10 ~sw net
      (`Tcp (Net.Ipaddr.V6.any, 10001))
  in
  let remote =
    Net.getaddrinfo_stream net "pestcontrol.protohackers.com"
    |> List.find_map (function
         | `Tcp (ip, _) -> Some (`Tcp (ip, 20547))
         | _ -> None)
    |> Option.get
  in
  let connect () = Net.connect ~sw net remote in
  while true do
    Net.accept_fork ~sw socket ~on_error:raise (handler ~connect)
  done
