type t = int

let compare = Int.compare
let id = ref 0

let next () =
  let v = !id in
  incr id;
  v

let to_int = Fun.id
let of_int = Fun.id
