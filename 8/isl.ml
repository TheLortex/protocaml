open Eio

type cipher = Rev | Xor of int | Xorpos | Add of int | Addpos

let reverse_bits v =
  let res = ref 0 in
  for i = 0 to 7 do
    if v land (1 lsl i) > 0 then res := (2 * !res) + 1 else res := 2 * !res
  done;
  !res

let reverse_bits = Array.init 256 reverse_bits

let () =
  assert (reverse_bits.(0) = 0);
  assert (reverse_bits.(255) = 255);
  assert (reverse_bits.(1) = 128);
  assert (reverse_bits.(2) = 64);
  assert (reverse_bits.(3) = 128 + 64)

let rec parse_cipher b acc =
  let next = Buf_read.any_char b in
  match next with
  | '\000' -> List.rev acc
  | c ->
      let next =
        match c with
        | '\001' -> Rev
        | '\002' -> Xor (Buf_read.any_char b |> Char.code)
        | '\003' -> Xorpos
        | '\004' -> Add (Buf_read.any_char b |> Char.code)
        | '\005' -> Addpos
        | _ -> failwith "unk"
      in
      parse_cipher b (next :: acc)

let pmod a n = ((a mod n) + n) mod n

let encode_instr ~pos c = function
  | Rev -> reverse_bits.(pmod c 256)
  | Xor n -> c lxor n
  | Xorpos -> c lxor pos
  | Add n -> c + n
  | Addpos -> c + pos

let rec encode ~pos c = function
  | [] -> c
  | instr :: next -> encode ~pos (encode_instr ~pos c instr) next

let decode_instr ~pos c = function
  | Rev -> reverse_bits.(pmod c 256)
  | Xor n -> c lxor n
  | Xorpos -> c lxor pos
  | Add n -> c - n
  | Addpos -> c - pos

let rec decode ~pos c = function
  | [] -> c
  | instr :: next ->
      let d = decode ~pos c next in
      decode_instr ~pos d instr

let check_valid_cipher cipher =
  List.init 256 Fun.id
  |> List.exists (fun c -> pmod (decode ~pos:127 c cipher) 256 <> c)

let () =
  assert (not (check_valid_cipher []));
  assert (not (check_valid_cipher [ Xor 0 ]));
  assert (not (check_valid_cipher [ Xor 2; Xor 2 ]));
  assert (not (check_valid_cipher [ Rev; Rev ]))

let server flow =
  let b = Buf_read.of_flow ~max_size:1500 flow in
  let cipher = parse_cipher b [] in
  if not (check_valid_cipher cipher) then failwith "Invalid cipher"
  else
    let pos_in = ref 0 in
    let pos_out = ref 0 in
    let encode c =
      let c = Char.chr (pmod (encode ~pos:!pos_out (Char.code c) cipher) 256) in
      incr pos_out;
      c
    in
    let decode c =
      let c = Char.chr (pmod (decode ~pos:!pos_in (Char.code c) cipher) 256) in
      incr pos_in;
      c
    in
    object
      inherit Eio.Flow.two_way
      method shutdown = flow#shutdown

      method read_into cstruct =
        let l = flow#read_into cstruct in
        for i = 0 to l - 1 do
          Cstruct.set_char cstruct i (decode (Cstruct.get_char cstruct i))
        done;
        l

      method copy src =
        let msg = Cstruct.create_unsafe 1500 in
        try
          while true do
            let len = src#read_into msg in
            let cst = Cstruct.sub msg 0 len in
            let data = Cstruct.map encode cst in
            flow#write [ data ]
          done
        with End_of_file -> ()
    end
