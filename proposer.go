package main

import (
	"strings"
)

//--- Proposer Role Data structures and Methods ---//
type ProposeReq struct {
	Command Command
}
type ProposeResp struct {
	Okay bool
}

/*   Proposer code below is built off of this pseudo-code
proposer(v):
    while not decided:
	    choose n, unique and higher than any n seen so far
	    send prepare(n) to all servers including self
	    if prepare_ok(n, na, va) from majority:
	      v' = va with highest na; choose own v otherwise
	      send accept(n, v') to all
	      if accept_ok(n) from majority:
	        send decided(v') to all

	acceptor state on each node (persistent):
	 np     --- highest prepare seen
	 na, va --- highest accept seen
*/

func (r *Replica) Propose(receive ProposeReq, reply *ProposeResp) error {
	r.Mutex.RLock()
	sleepTime := 5 // measured in ms
	round := 1
	highestN := 0
	slot := Slot{Index: 0, Sequence: Sequence{N: 0, Address: r.Cell[0]}}
	vCommand := receive.Command
	vaCommand := Command{}
	numTrue, numFalse := 0, 0

	//Find first undecided slot
	undecidedSlotFound := false
	for i := 0; i < len(r.Slots); i++ {
		if !r.Slots[i].Decided {
			slot = r.Slots[i]
			undecidedSlotFound = true
			break
		}
	}
	if !undecidedSlotFound {
		r.getSlots(len(r.Slots))
		slot.Index = len(r.Slots) - 1
	}
	//while not decided
	for {
		chatf(1, "Propose: Round: %d", round)
		r.getSlots(slot.Index)

		//Check to see if the slot has been decided
		if r.Slots[slot.Index].Decided {
			if r.Slots[slot.Index].Command.Tag == receive.Command.Tag {
				r.sendCommand(vCommand)
				return nil
			}
			highestN = 0
			vCommand = receive.Command
			slot.Sequence = Sequence{N: 0, Address: r.Cell[0]}
			slot.Command = Command{}
			slot.Accepted = false
			slot.Decided = false
			vaCommand = Command{Command: ""}
			numTrue, numFalse = 0, 0
			slot.Index = slot.Index + 1
			chatf(1, "Propose: Slot already decided moving slot index to: %d", slot.Index)
		}

		chatf(1, "Propose: Proposing on slot #: %d", slot.Index)

		//choose n, unique and higher than any n seen so far
		n := slot.Sequence.N + 1

		if n <= highestN {
			n = highestN + 1
		}

		//send prepare(n) to all servers including self
		response := make(chan PrepareResp, len(r.Cell))
		r.Mutex.RUnlock()
		for _, address := range r.Cell {
			go func(address Address, slotIndex int, n int, response chan PrepareResp) {
				send := PrepareReq{slotIndex, Sequence{N: n, Address: r.Cell[0]}}
				recv := PrepareResp{}
				RandLatency()
				Call(address.String(), "Replica.Prepare", send, &recv)
				RandLatency()
				response <- recv
			}(address, slot.Index, n, response)
		}
		r.Mutex.RLock()

		//Process prepare responses
		for i := 0; i < len(r.Cell); i++ {
			prepareResp := <-response
			if prepareResp.Okay {
				numTrue++
			} else {
				numFalse++
			}
			//New highest n value returned
			if prepareResp.Promised.N > highestN {
				chatf(1, "Propose: New highest n returned from prepare N: %d, Address: %s", prepareResp.Promised.N, prepareResp.Promised.Address.String())
				highestN = prepareResp.Promised.N
				//New highest command was accepted
				if prepareResp.Command.Command != "" {
					vaCommand = prepareResp.Command
					chatf(1, "Propose: New highest command returned from prepare %s", prepareResp.Command.Command)
				}
			}
			//A majority was reached - exit loop
			if r.majority(numTrue) || r.majority(numFalse) {
				break
			}
		}
		//Check to see if a decision was made during prepare phase
		if r.Slots[slot.Index].Decided {
			if r.Slots[slot.Index].Command.Tag == receive.Command.Tag {
				r.sendCommand(vCommand)
				return nil
			}
			chatf(1, "Propose: Slot already decided moving slot index to: %d", slot.Index)
			continue
		}

		//if prepare_ok(n, na, va) from majority
		if r.majority(numTrue) {
			chatf(1, "Propose: Got a majority of 'true' votes from Prepare")
			var vprime AcceptReq
			//v' = va with highest na; choose own v otherwise
			if vaCommand.Command != "" {
				vprime = AcceptReq{Slot: slot.Index, Sequence: Sequence{N: highestN, Address: r.Cell[0]}, Command: vaCommand}
			} else { //No highest command returned from prepare - use value passed into Propose()
				vprime = AcceptReq{Slot: slot.Index, Sequence: Sequence{N: highestN, Address: r.Cell[0]}, Command: vCommand}
			}

			//send accept(n, v') to all
			acceptResponse := make(chan AcceptResp, len(r.Cell))
			r.Mutex.RUnlock()
			for _, address := range r.Cell {
				go func(address Address, accreq AcceptReq, response chan AcceptResp) {
					recv := AcceptResp{}
					RandLatency()
					Call(address.String(), "Replica.Accept", accreq, &recv)
					RandLatency()
					acceptResponse <- recv
				}(address, vprime, acceptResponse)
			}
			r.Mutex.RLock()

			numTrue = 0
			numFalse = 0
			//Process accept responses
			for i := 0; i < len(r.Cell); i++ {
				acceptResp := <-acceptResponse
				if acceptResp.Okay {
					numTrue++
				} else {
					numFalse++
				}
				if acceptResp.Promised > highestN {
					chatf(1, "Propose: New highest n returned from accept N: %d", acceptResp.Promised)
					highestN = acceptResp.Promised
				}

				if r.majority(numTrue) || r.majority(numFalse) {
					break
				}
			}
			//Check to see if a decision was made during accept phase
			if r.Slots[slot.Index].Decided {
				if r.Slots[slot.Index].Command.Tag == receive.Command.Tag {
					r.sendCommand(vCommand)
					return nil
				}
				chatf(1, "Propose: Slot already decided moving slot index to: %d", slot.Index)
				continue
			}

			//if accept_ok(n) from majority:
			if r.majority(numTrue) {
				chatf(1, "Propose: Got a majority of 'true' votes from Accept")
				r.Mutex.RUnlock()
				//send decided(v') to all
				for _, address := range r.Cell {
					go func(address Address, slotIndex int, command Command) {
						send := DecideReq{slotIndex, command}
						recv := DecideResp{}
						RandLatency()
						Call(address.String(), "Replica.Decide", send, &recv)
						RandLatency()
					}(address, slot.Index, vprime.Command)
				}
				r.Mutex.RLock()
				//Other commands need to be processed
				if vaCommand.Tag > 0 && vaCommand.Tag != vCommand.Tag {
					round++
					continue
				} else { //This was the command passed in - exit Propose()
					break
				}
			} else {
				chatf(1, "Propose: Did not get a majority of 'true' votes from Accept... restarting")
				RandLatency(sleepTime)
				sleepTime *= 2
				round++
				continue
			}
		} else {
			chatf(1, "Propose: Did not get a majority of 'true' votes from Prepare... restarting")
			RandLatency(sleepTime)
			sleepTime *= 2
			round++
			continue
		}
	}
	r.Mutex.RUnlock()
	return nil
}

func (r *Replica) majority(n int) bool {
	if n*2 > len(r.Cell) {
		return true
	}
	return false
}

func (r *Replica) sendCommand(cmd Command) {
	commandResponse := ""
	commandTokens := strings.Split(cmd.Command, " ")
	if commandTokens[0] == "put" {
		commandResponse = "[" + commandTokens[1] + "] => " + commandTokens[2] + " added to database"
	} else if commandTokens[0] == "get" {
		commandResponse = "[" + commandTokens[1] + "] => " + r.Database[commandTokens[1]]
	} else if commandTokens[0] == "delete" {
		commandResponse = "\"" + commandTokens[1] + "\" deleted from database"
	}
	//Set a response value for the listener channel listening in main()
	_, ok := r.Listeners[cmd.Key]
	if ok {
		r.Listeners[cmd.Key] <- commandResponse
	}
}
