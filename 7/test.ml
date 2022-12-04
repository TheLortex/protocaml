open Eio

let handler ~clock ~sw flow _ =
  Logs.info (fun f -> f "New connection!");
  (try
     let buf = Buf_read.of_flow ~max_size:200000 flow in
     while true do
       let line = Buf_read.line buf in
       Logs.info (fun f -> f ">>> %s" line);
       let rev =
         line |> String.to_seq |> List.of_seq |> List.cons '\n' |> List.rev
         |> List.to_seq |> String.of_seq
       in
       Logs.info (fun f -> f "<<< %s" rev);
       Flow.copy_string rev flow
     done
   with End_of_file -> ());
  Logs.info (fun f -> f "The end")

let c = Mtime_clock.counter ()

let reporter ppf =
  let report src level ~over k msgf =
    let k _ =
      over ();
      k ()
    in
    let with_stamp h tags k ppf fmt =
      Format.kfprintf k ppf
        ("%a[%a] @[" ^^ fmt ^^ "@]@.")
        Logs.pp_header (level, h) Mtime.Span.pp (Mtime_clock.count c)
    in
    msgf @@ fun ?header ?tags fmt -> with_stamp header tags k ppf fmt
  in
  { Logs.report }

let () =
  Fmt_tty.setup_std_outputs ();
  Logs.set_level (Some Info);
  Logs.set_reporter (reporter Format.std_formatter);
  Eio_linux.run ~queue_depth:300 @@ fun env ->
  Switch.run @@ fun sw ->
  let net = Stdenv.net env in
  let clock = Stdenv.clock env in
  let socket = Net.datagram_socket ~sw net (`Udp (Net.Ipaddr.V6.any, 10000)) in
  let lrcp = Lrcp.listen (socket :> Net.datagram_socket) in
  while true do
    Net.accept_fork ~sw lrcp ~on_error:raise (handler ~clock ~sw)
  done
