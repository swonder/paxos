package main

import (
	"strings"
	"time"
)

//--- Learner Role Data structures and Methods ---//
type DecideReq struct {
	Slot    int
	Command Command
}
type DecideResp struct {
	Success bool
}

//Decide(slot, command)
func (r *Replica) Decide(receive DecideReq, reply *DecideResp) error {
	r.Mutex.RLock()
	defer r.Mutex.RUnlock()

	if r.Slots[receive.Slot].Decided && (r.Slots[receive.Slot].Command.Command != receive.Command.Command) {
		panic("Decide: Value has already been decided and it is different from received value")
	}

	if r.Slots[receive.Slot].Decided {
		chatf(2, "Decide: This slot has already been decided")
		reply.Success = false
	}

	r.Slots[receive.Slot].Decided = true
	chatf(2, "Decide: \"%s\" has been decided.", receive.Command.Command)

	//First time this slot has been decided - can decision be applied?
	for !everySlotDecided(r.Slots, receive.Slot) {
		time.Sleep(time.Second)
	}

	var commandResponse string
	//Update the database
	commandTokens := strings.Split(receive.Command.Command, " ")
	if commandTokens[0] == "put" {
		r.Database[commandTokens[1]] = commandTokens[2]
		commandResponse = "[" + commandTokens[1] + "] => " + commandTokens[2] + " added to database"
	} else if commandTokens[0] == "get" {
		commandResponse = "[" + commandTokens[1] + "] => " + r.Database[commandTokens[1]]
	} else if commandTokens[0] == "delete" {
		dBaseVal := r.Database[commandTokens[1]]
		delete(r.Database, commandTokens[1])
		commandResponse = "[" + commandTokens[1] + "] => " + dBaseVal + " deleted from database"
	} else {
		commandResponse = "Unrecoginized command"
	}

	//Set a response value for the listener channel listening in main()
	//so main() can continue on
	_, ok := r.Listeners[receive.Command.Key]
	if ok {
		r.Listeners[receive.Command.Key] <- commandResponse
	}
	reply.Success = true
	return nil
}
