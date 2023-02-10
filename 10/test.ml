open Eio

module Command = struct
  type t =
    | Help
    | Get of { file : Fpath.t; revision : string option }
    | List of { path : Fpath.t }
    | Put of { file : Fpath.t; content : string }

  let filename_is_valid v =
    (not (Astring.String.is_infix ~affix:"//" v))
    && v.[0] = '/'
    && String.for_all
         (function
           | '0' .. '9' | 'a' .. 'z' | 'A' .. 'Z' | '_' | '-' | '/' | '.' ->
               true
           | _ -> false)
         v

  let content_is_valid v =
    String.for_all
      (fun c -> Astring.Char.Ascii.(is_print c) || c = '\n' || c = '\t')
      v

  let parse i =
    let s = Buf_read.line i in
    Logs.info (fun f -> f "<<%s" s);
    match String.split_on_char ' ' s with
    | [] -> Error "Provide a command"
    | command :: rest -> (
        match (String.uppercase_ascii command, rest) with
        | "HELP", _ -> Ok Help
        | "GET", [ file ] -> Ok (Get { file = Fpath.v file; revision = None })
        | "GET", [ file; revision ] ->
            Ok (Get { file = Fpath.v file; revision = Some revision })
        | "PUT", [ file; length ] -> (
            match int_of_string_opt length with
            | Some length ->
                if filename_is_valid file then (
                  let content = Buf_read.take length i in
                  Logs.info (fun f -> f "<<%S" content);
                  if content_is_valid content then
                    Ok (Put { file = Fpath.v file; content })
                  else Error "invalid content")
                else Error "invalid file name"
            | None -> Error "Invalid length")
        | "LIST", [ path ] -> Ok (List { path = Fpath.v path })
        | _ -> Error "Invalid command")
end

module Response = struct
  type t =
    | Err of string
    | OkGet of { content : string }
    | OkPut of { revision : string }
    | OkList of (string * string) list
    | OkHelp

  let serialize_response s =
    let open Buf_write in
    function
    | Err str ->
        string s "ERR ";
        string s str;
        char s '\n'
    | OkGet { content } ->
        string s "OK ";
        string s (string_of_int (String.length content));
        char s '\n';
        string s content
    | OkList lst ->
        string s "OK ";
        string s (List.length lst |> string_of_int);
        string s "\n";
        List.iter
          (fun (a, b) ->
            string s a;
            char s ' ';
            string s b;
            char s '\n')
          lst
    | OkPut { revision } ->
        string s "OK ";
        string s revision;
        char s '\n'
    | OkHelp -> string s "OK usage: HELP|GET|PUT|LIST\n"
end

type entry = { name : string; node : node }
and node = File of (string, string) Hashtbl.t | Dir of entry list ref

let list_node_info v =
  match v.node with
  | File l -> (v.name, "r" ^ (Hashtbl.length l |> string_of_int))
  | Dir _ -> (v.name ^ "/", "DIR")

let find root path =
  if Fpath.is_abs path then
    let segs = Fpath.segs path |> List.filter (fun v -> v <> "") in
    let rec loop root segs =
      match (segs, root) with
      | [], _ -> Ok root
      | _, File _ -> Error "not found"
      | a :: b, Dir entries -> (
          match List.find_opt (fun x -> x.name = a) !entries with
          | None -> Error "not found"
          | Some v -> loop v.node b)
    in
    loop root segs
  else Error "invalid path"

let ls root path =
  let open Response in
  match find root path with
  | Error "invalid path" -> Error "illegal dir name"
  | Error "not found" -> Ok (OkList [])
  | Error v -> Error v
  | Ok (Dir entries) ->
      Ok
        (OkList
           (List.map list_node_info !entries
           |> List.sort_uniq (fun (a, _) (b, _) -> String.compare a b)))
  | Ok (File _) -> Ok (OkList [])

let last_revision_content v =
  Hashtbl.find v ("r" ^ (Hashtbl.length v |> string_of_int))

let get root path revision =
  let open Response in
  match (find root path, revision) with
  | Error "invalid path", _ -> Error "illegal file name"
  | Error "not found", _ -> Error "no such file"
  | Error v, _ -> Error v
  | Ok (Dir _), _ -> Error "found a directory"
  | Ok (File hsh), Some revision -> (
      match Hashtbl.find_opt hsh revision with
      | None -> Error "no such revision"
      | Some v -> Ok (OkGet { content = v }))
  | Ok (File hsh), None -> Ok (OkGet { content = last_revision_content hsh })

let is_dir = function Dir _ -> true | _ -> false

let mkfile root path content =
  let open Response in
  let name = Fpath.basename path in
  let segs = Fpath.segs (Fpath.parent path) |> List.filter (fun v -> v <> "") in
  let rec loop root segs =
    match (segs, root) with
    | [], File _ -> Error "internal"
    | [], Dir entries ->
        let file = Hashtbl.create 1 in
        Hashtbl.add file "r1" content;
        entries := { name; node = File file } :: !entries;
        Ok (OkPut { revision = "r1" })
    | _ :: _, File _ -> Error "internal"
    | a :: rest, Dir entries -> (
        match List.find_opt (fun e -> e.name = a && is_dir e.node) !entries with
        | None ->
            let node = Dir (ref []) in
            let new_entry = { name = a; node } in
            entries := new_entry :: !entries;
            loop node rest
        | Some entry -> loop entry.node rest)
  in
  loop root segs

let put root path content =
  match find root path with
  | Error "invalid path" -> Error "invalid file name"
  | Error "not found" -> mkfile root path content
  | Error v -> Error v
  | Ok (Dir _) -> Error "is directory"
  | Ok (File hsh) ->
      let old_content = last_revision_content hsh in
      if old_content = content then
        let len = Hashtbl.length hsh in
        let rev = "r" ^ string_of_int len in
        Ok (OkPut { revision = rev })
      else
        let len = Hashtbl.length hsh in
        let rev = "r" ^ string_of_int (len + 1) in
        Hashtbl.add hsh rev content;
        Ok (OkPut { revision = rev })

let execute state =
  let open Response in
  function
  | Command.Help -> Ok OkHelp
  | List { path } -> ls state path
  | Get { file; revision } -> get state file revision
  | Put { file; content } -> put state file content

let root = Dir (ref [])

let handler flow _ =
  let input = Buf_read.of_flow ~max_size:10_000_000 flow in
  try
    Buf_write.with_flow flow (fun write ->
        while true do
          Logs.info (fun f -> f "<<READY");
          Flow.copy_string "READY\n" flow;
          let instr = Command.parse input in
          let res = Result.bind instr (execute root) in
          let resp = match res with Ok v -> v | Error msg -> Err msg in
          Response.serialize_response write resp;
          Buf_write.flush write
        done)
  with End_of_file | Eio.Net.Connection_reset _ -> ()

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
  while true do
    Net.accept_fork ~sw socket ~on_error:raise handler
  done
