open Eio

module Toys = struct
  type t = { quantity : int; name : string }

  let of_string x =
    match String.split_on_char ' ' x with
    | [] | [ _ ] -> assert false
    | quantity :: name ->
        assert (quantity.[String.length quantity - 1] = 'x');
        {
          quantity =
            int_of_string (String.sub quantity 0 (String.length quantity - 1));
          name = String.concat " " name;
        }

  let to_string x = Fmt.str "%dx %s" x.quantity x.name
  let compare a b = Int.compare a.quantity b.quantity
end

let handler ~clock ~sw flow _ =
  Logs.info (fun f -> f "New connection!");
  (try
     let buf = Buf_read.of_flow ~max_size:200000 flow in
     while true do
       let line = Buf_read.line buf in
       Logs.info (fun f -> f ">>> %s" line);

       let toys_req =
         line |> String.split_on_char ',' |> List.map Toys.of_string
         |> List.sort Toys.compare |> List.rev
       in
       let rev = List.hd toys_req |> Toys.to_string in
       Logs.info (fun f -> f "<<< %s" rev);
       Flow.copy_string (rev ^ "\n") flow
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
  let socket =
    Net.listen ~backlog:10 ~reuse_addr:true ~sw net
      (`Tcp (Net.Ipaddr.V6.any, 10000))
  in
  while true do
    Net.accept_fork ~sw socket ~on_error:raise (fun flow s ->
        try
          let secure_flow = Isl.server flow in
          handler ~clock ~sw secure_flow s
        with Failure msg ->
          Logs.err (fun f -> f "==> %s" msg);
          Eio.Flow.shutdown flow `All)
  done
