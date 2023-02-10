let c = Mtime_clock.counter ()

let reporter ppf =
  let report _src level ~over k msgf =
    let k _ =
      over ();
      k ()
    in
    let with_stamp h _tags k ppf fmt =
      Format.kfprintf k ppf
        ("%a[%a] @[" ^^ fmt ^^ "@]@.")
        Logs.pp_header (level, h) Mtime.Span.pp (Mtime_clock.count c)
    in
    msgf @@ fun ?header ?tags fmt -> with_stamp header tags k ppf fmt
  in
  { Logs.report }

let init () =
  Fmt_tty.setup_std_outputs ();
  Logs.set_level (Some Error);
  Logs.set_reporter (reporter Format.std_formatter)