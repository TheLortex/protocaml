module Entry = struct
  type t = { job : Yojson.Safe.t; id : Job_id.t }

  let compare a b = Job_id.compare a.id b.id
end

module Prio = struct
  type t = int

  let compare a b = -Int.compare a b
end

module Psq = Psq.Make (Entry) (Prio)

type q = { mutable q : Psq.t; cond : Eio.Condition.t; name : string }

type t = {
  queues : (string, q) Hashtbl.t;
  id_to_q : (Job_id.t, q) Hashtbl.t;
  pending : (Job_id.t, unit) Hashtbl.t;
}

let st =
  {
    queues = Hashtbl.create 10;
    id_to_q = Hashtbl.create 10;
    pending = Hashtbl.create 10;
  }

let get_or_create id =
  match Hashtbl.find_opt st.queues id with
  | Some q -> q
  | None ->
      let q = { q = Psq.empty; cond = Eio.Condition.create (); name = id } in
      Hashtbl.add st.queues id q;
      q

let put ~name job pri =
  let id = Job_id.next () in
  let v = get_or_create name in
  v.q <- Psq.add { id; job } pri v.q;
  Hashtbl.add st.id_to_q id v;
  Eio.Condition.broadcast v.cond;
  id

let put_back (job : Protocol.get_response) =
  let v = get_or_create job.queue in
  v.q <- Psq.add { id = job.id; job = job.job } job.pri v.q;
  Hashtbl.add st.id_to_q job.id v;
  Hashtbl.remove st.pending job.id;
  Eio.Condition.broadcast v.cond

let delete job_id =
  if Hashtbl.mem st.pending job_id then (
    Hashtbl.remove st.pending job_id;
    true)
  else
    try
      let v = Hashtbl.find st.id_to_q job_id in
      v.q <- Psq.remove { id = job_id; job = `Assoc [] } v.q;
      Hashtbl.remove st.id_to_q job_id;
      true
    with Not_found -> false

let ( let+ ) a b = Option.map b a

let get_opt queues =
  let+ res =
    List.to_seq queues
    |> Seq.filter_map (fun queue ->
           let v = get_or_create queue in
           let+ entry, pri = Psq.min v.q in
           { Protocol.job = entry.job; id = entry.id; pri; queue })
    |> Seq.fold_left
         (fun (res : Protocol.get_response option) (v : Protocol.get_response) ->
           match res with Some v1 when v1.pri > v.pri -> Some v1 | _ -> Some v)
         None
  in
  let v = get_or_create res.queue in
  v.q <- Psq.rest v.q |> Option.get;
  Hashtbl.remove st.id_to_q res.id;
  Hashtbl.add st.pending res.id ();
  res

let rec get_wait queues =
  match get_opt queues with
  | Some v -> v
  | None ->
      let queues' = List.map get_or_create queues in
      Eio.Fiber.any
        (List.map (fun v () -> Eio.Condition.await_no_mutex v.cond) queues');
      get_wait queues

let is_pending job = Hashtbl.mem st.pending job
