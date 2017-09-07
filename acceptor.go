package main

import ()

//--- Acceptor Role Data structures and Methods ---//
type PrepareReq struct {
	Slot int
	N    Sequence
}
type PrepareResp struct {
	Okay     bool
	Promised Sequence
	Command  Command
}

// Prepare(slot, seq) -> (okay, promised, command):
func (r *Replica) Prepare(receive PrepareReq, reply *PrepareResp) error {
	r.Mutex.RLock()
	defer r.Mutex.RUnlock()
	r.getSlots(receive.Slot)

	if r.Slots[receive.Slot].Decided {
		chatf(2, "Prepare: Slot %d has aleady been decided", r.Slots[receive.Slot].Index)
	}
	seqcmp := receive.N.Cmp(r.Slots[receive.Slot].Sequence)
	if seqcmp > 0 { //A new highest sequence has been propopsed
		chatf(2, "Prepare: A new highest number proposal has been received. Replica n: %d, Received n: %d", r.Slots[receive.Slot].Sequence.N, receive.N.N)
		r.Slots[receive.Slot].Sequence = receive.N
		reply.Okay = true
		reply.Promised = r.Slots[receive.Slot].Sequence
		reply.Command = r.Slots[receive.Slot].Command
	} else { //Higher sequence has been promised
		chatf(2, "Prepare: Already promised a higher sequence number. Replica n: %d, Received n: %d", r.Slots[receive.Slot].Sequence.N, receive.N.N)
		reply.Okay = false
		reply.Promised = r.Slots[receive.Slot].Sequence
	}
	return nil
}

type AcceptReq struct {
	Slot     int
	Sequence Sequence
	Command  Command
}
type AcceptResp struct {
	Okay     bool
	Promised int
}

// Accept(slot, seq, command) -> (okay, promised):
func (r *Replica) Accept(receive AcceptReq, reply *AcceptResp) error {
	r.Mutex.RLock()
	defer r.Mutex.RUnlock()
	r.getSlots(receive.Slot)

	seqcmp := receive.Sequence.Cmp(r.Slots[receive.Slot].Sequence)
	if seqcmp <= 0 || r.Slots[receive.Slot].Sequence.N == 0 { //Accept the value
		r.Slots[receive.Slot].Command = receive.Command
		r.Slots[receive.Slot].Accepted = true
		reply.Okay = true
		reply.Promised = r.Slots[receive.Slot].Sequence.N
		chatf(2, "Accept: Command accepted. Received n: %d, Replica n: %d", receive.Sequence.N, r.Slots[receive.Slot].Sequence.N)
	} else { //Don't accept the value because a higher sequence has been promised
		reply.Okay = false
		reply.Promised = r.Slots[receive.Slot].Sequence.N
		chatf(2, "Accept: Command not accepted. Replica had higher sequence value for this slot. Received n: %d, Replica n: %d", receive.Sequence.N, r.Slots[receive.Slot].Sequence.N)
	}
	return nil
}
