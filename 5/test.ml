open Eio

let is_boguscoin str =
  let l = String.length str in
  l >= 26 && l <= 35 && str.[0] = '7'  

let tony = "7YWHMfk9JZe0LM0g1ZauHuiSxhI"

let rewrite s input output = 
  let reader = Buf_read.of_flow ~max_size:1_000_000 input in
  try 
    while true do
      (* for each line, we split into words and map those that matches the 
         boguscoin spec. The tricky part when using the buffered reader is to 
         know whether the last line before EOF is actually ending with a \n. *)
      let line = Buf_read.take_while (fun c -> c <> '\n') reader in
      let at_end_of_input = Buf_read.at_end_of_input reader in
      Printf.printf "|%s %S (%b)\n%!" s line at_end_of_input;
      (* the message is transformed word by word*)
      let line =
        String.split_on_char ' ' line 
        |> List.map (fun l -> if is_boguscoin l then tony else l)
        |> String.concat " "
      in
      Printf.printf "|%s %S\n%!" s line;
      if at_end_of_input then
        begin
          Flow.copy_string line output;
          raise End_of_file
        end
      else
        begin
          Buf_read.char '\n' reader;
          Flow.copy_string (line ^ "\n") output
        end
    done
  with
  | End_of_file -> ()

let handler ~sw net flow _ =
  let upstream = 
    Net.connect ~sw net 
    (`Tcp (Eio_unix.Ipaddr.of_unix 
      (Unix.inet_addr_of_string "206.189.113.124"), 
       16963)) 
  in
  Fiber.both
    (fun () -> 
      rewrite "<<" upstream flow;
      Printf.printf "end from upstream \n%!";
      Flow.shutdown flow `All)
    (fun () -> 
      rewrite ">>" flow upstream;
      Printf.printf "end from client \n%!";
      Flow.shutdown upstream `All)
  
let () =
  Eio_main.run @@ fun env ->
  Switch.run @@ fun sw ->
  let net = Stdenv.net env in
  let socket = Net.listen 
    ~reuse_addr:true ~backlog:10 ~sw net 
    (`Tcp (Net.Ipaddr.V6.any, 10000)) 
  in
  while true do
    Net.accept_fork ~sw socket ~on_error:ignore (handler ~sw net)
  done
