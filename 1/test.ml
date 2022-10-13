open Eio

(* 1: Prime Time
   
To keep costs down, a hot new government department is contracting out its 
mission-critical primality testing to the lowest bidder. (That's you).

Officials have devised a JSON-based request-response protocol. Each request is 
a single line containing a JSON object, terminated by a newline character 
('\n', or ASCII 10). Each request begets a response, which is also a single 
line containing a JSON object, terminated by a newline character.

After connecting, a client may send multiple requests in a single session. 
Each request should be handled in order.

A conforming request object has the required field method, which must always 
contain the string "isPrime", and the required field number, which must contain 
a number. Any JSON number is a valid number, including floating-point values.

(...)
*)


(* this API has to support arbitrarily large integers, so the zarith library 
    has to be used. opening the Z module shadows most integer operations so 
    it's mostly transparent. *)
let is_prime n =
  let open Z in
  let rec no_divisors m =
    m * m > n || (n mod m != Z.zero && no_divisors (m + Z.one))
  in
  n >= Z.of_int 2 && no_divisors (Z.of_int 2)

(* check if a number is prime and respond using the specified format *)
let respond ~flow number =
  let response =
    Fmt.str
      {|{"method":"isPrime","prime":%b}%s|}
    (is_prime number) "\n"
  in
  Flow.copy_string response flow


exception Break

(* parse the json and checks that we are indeed requesting the correct method.
   the `yojson` library is used for that purpose. *)
let handle_line ~flow line =
  let req = Yojson.Safe.from_string line in
  let open Yojson.Safe.Util in
  let meth = req |> member "method" |> to_string in
  let number = 
    match req |> member "number" with 
    | `Int n -> Z.of_int n
    | `Intlit s -> Z.of_string s
    | `Float _ -> Z.zero
    | _ -> raise Break
  in
  match meth with
  | "isPrime" -> respond ~flow number
  | _ -> raise Break

(* the connection handler simply read lines and handle them as long as data 
   is available. on exceptions, a malformed response is sent and the connection 
   is closed *)
let handler flow _ =
  let buffered_reader = Buf_read.of_flow ~max_size:1_000_000 flow in
  let rec loop () =
    match Buf_read.line buffered_reader with
    | line -> handle_line ~flow line |> ignore; loop ()
    | exception End_of_file -> ()
  in
  try loop () with
  | Yojson.Json_error _ 
  | Yojson.Safe.Util.Type_error _ 
  | Break -> 
    Eio.Flow.copy_string "\n" flow

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
