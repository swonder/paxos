package main

import (
	"bytes"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"strconv"
	"sync"
)

//--- Replica data structures and methods that are not directly part of
//    Acceptor, Learner, or Proposer Roles

// ADDRESS STRUCT AND METHODS
type Address struct {
	IP   string
	Port string
}

func (a *Address) String() string {
	return net.JoinHostPort(a.IP, a.Port)
}
func (a *Address) Print() {
	fmt.Println(a.String())
}

// SLOT STRUCT AND METHODS
type Slot struct {
	Index    int
	Sequence Sequence
	Command  Command
	Accepted bool
	Decided  bool
}

func (s *Slot) String() string {
	return fmt.Sprintf("Index: %d, Sequence: %d, Command: %d, Accepted: %t, Decided: %t", s.Index, s.Sequence.String(), s.Command.String, s.Accepted, s.Decided)
}
func (s *Slot) Print() {
	fmt.Println(s.String())
}

// SEQUENCE STRUCT AND METHODS
type Sequence struct {
	N       int
	Address Address
}

func (s *Sequence) String() string {
	return fmt.Sprintf("Address: %s, Sequence # %d", s.Address.String(), s.N)
}
func (s *Sequence) Print() {
	fmt.Printf("Address: %s, Sequence # %d", s.Address.String(), s.N)
}
func (this *Sequence) Cmp(that Sequence) int {
	if this.N < that.N {
		return -1
	} else if this.N > that.N {
		return 1
	} else {
		if this.Address.IP < that.Address.IP {
			return -1
		} else if this.Address.IP > that.Address.IP {
			return 1
		}
		return 0
	}
}

// COMMAND STRUCT AND METHODS
type Command struct {
	Promise Sequence
	Command string
	Address Address
	Tag     int
	Key     string
}

func (c *Command) String() string {
	return fmt.Sprintf("%s", c.Command)
}
func (c *Command) Print() {
	fmt.Printf("Promise: %s, Command: %s, Address: %s, Tag: %d\n", c.Promise.String(), c.Command, c.Address, c.Tag)
}
func (this *Command) Equal(that Command) bool {
	return this.Promise.Cmp(that.Promise) == 0 && this.Tag == that.Tag
}

//REPLICA STRUCT AND METHODS
type Replica struct {
	Cell      []Address //Cell[0] must always be the local address/port
	Slots     []Slot
	Database  map[string]string
	Listeners map[string]chan string
	Mutex     sync.RWMutex
}

func CreateReplica(cell []string) *Replica {
	var addresses []Address
	//Format and insert addresses passed in from command line to Address{} structs
	for _, v := range cell {
		host, port, err := net.SplitHostPort(v)
		if err != nil {
			fmt.Println(err)
		}
		//If host is empty assume user was refering to port on local address
		if host == "" {
			host = GetLocalAddress()
		}
		addresses = append(addresses, Address{IP: host, Port: port})
	}

	fmt.Println("Creating RPC server for new node...")
	return &Replica{
		Cell:      addresses,
		Database:  make(map[string]string),
		Listeners: make(map[string]chan string)}
}

func Listen(r *Replica) {
	rpc.Register(r)
	rpc.HandleHTTP()
	fmt.Printf("RPC server is listening on port: %s\n", r.Cell[0].String())
	l, e := net.Listen("tcp", ":"+r.Cell[0].Port)
	if e != nil {
		log.Fatal("Listen: Listen error:", e)
	}
	go http.Serve(l, nil)
}

func (r *Replica) Ping(_ Nothing, reply *int) error {
	*reply = 562
	return nil
}

func (r *Replica) Dump(_ Nothing, reply *string) error {
	r.Mutex.RLock()
	defer r.Mutex.RUnlock()
	i := 0
	var buffer bytes.Buffer
	buffer.WriteString("\nCell Addresses:    \n")
	for _, cell := range r.Cell {
		if i == 0 {
			buffer.WriteString("     " + cell.String() + " (Local replica address)\n")
		} else {
			buffer.WriteString("     " + cell.String() + "\n")
		}
		i++
	}
	buffer.WriteString("\nSlots:    \n")
	for _, Slot := range r.Slots {
		buffer.WriteString(fmt.Sprintf("     [%d]=>\"%s\" N: %d/%s Accepted: %t Decided: %t\n", Slot.Index, Slot.Command.Command, Slot.Sequence.N, Slot.Sequence.Address.String(), Slot.Accepted, Slot.Decided))
		i++
	}
	buffer.WriteString("\n     # Slots filled: " + strconv.Itoa(len(r.Slots)) + "\n")
	buffer.WriteString("\nDatabase:        \n")
	for k, v := range r.Database {
		buffer.WriteString("     [" + k + "]: " + v + "\n")
	}
	buffer.WriteString("\n     # Database items: " + strconv.Itoa(len(r.Database)) + "\n")
	*reply = buffer.String()

	return nil
}

//Make sure slots exist up to 'n', but will not overwrite any existing slots
func (r *Replica) getSlots(n int) {
	for i := len(r.Slots); i <= n; i++ {
		sequence := Sequence{N: 0, Address: r.Cell[0]}
		slot := Slot{Index: i, Sequence: sequence, Command: Command{}, Accepted: false, Decided: false}
		r.Slots = append(r.Slots, slot)
	}
}
