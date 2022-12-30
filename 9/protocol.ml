
type put = { job : Yojson.Safe.t; pri : int; queue : string }
type id = { id : Job_id.t }
type get = { queues : string list; wait : bool }

type get_response = {
  id : Job_id.t;
  job : Yojson.Safe.t;
  pri : int;
  queue : string;
}

type 'a request =
  | Put : put -> id request
  | Get : get -> get_response request
  | Delete : id -> unit request
  | Abort : id -> unit request

type any_request = U : 'a request -> any_request
type 'a status = Ok of 'a | Error of string | NoJob
