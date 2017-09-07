package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"
)

var chatty,
	latency *int

type Nothing struct{}

var sendNothing Nothing
var returnNothing *Nothing

func main() {
	rand.Seed(time.Now().UTC().UnixNano())

	//Take care of the -chatty and -verbose commands first
	chatty = flag.Int("chatty", 0, "How verbose messages are")
	latency = flag.Int("latency", 0, "Simulated network latency")
	flag.Parse()

	if *chatty < 0 {
		*chatty = 0
	} else if *chatty > 2 {
		*chatty = 2
	}

	cell := flag.Args()
	if len(cell) < 1 {
		fmt.Println("Not enough replica addresses specified to create a cell")
		return
	}

	//If no colon is found, assume number given is a port and prepend colon to it
	for i := 0; i < len(cell); i++ {
		if !strings.Contains(cell[i], ":") {
			cell[i] = ":" + cell[i]
		}
	}

	fmt.Println("Welcome to Paxos v.1.1")
	fmt.Println("By Shawn Wonder")
	fmt.Println("Type 'help' for a list of commands\n")
	fmt.Println("Chattyness  : " + strconv.Itoa(*chatty))
	fmt.Println("Latency (ms): " + strconv.Itoa(*latency) + "\n")

	//Create the replica
	replica := CreateReplica(cell)
	Listen(replica)

	PrintPrompt()

	//Start reading lines of text that the user inputs
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		command := scanner.Text()
		commandTokens := strings.Fields(command)
		if len(commandTokens) > 0 {
			//Insert key and value into paxos - put <key> <value>
			if commandTokens[0] == "put" {
				if len(commandTokens) == 3 {
					command := Command{}
					command.Address = replica.Cell[0]
					command.Command = strings.Join(commandTokens, " ")
					command.Promise = Sequence{N: 0, Address: replica.Cell[0]}
					command.Tag = rand.Int()
					key := command.Address.IP + "-" + strconv.Itoa(command.Tag)
					command.Key = key

					responseChannel := make(chan string, 1)
					replica.Mutex.RLock()
					replica.Listeners[key] = responseChannel
					replica.Mutex.RUnlock()

					send := ProposeReq{Command: command}
					reply := ProposeResp{}
					RandLatency()
					Call(replica.Cell[0].String(), "Replica.Propose", send, &reply)
					RandLatency()
					fmt.Println(<-replica.Listeners[key])
				} else {
					fmt.Println("Number of arguments supplied incorrect - usage: put <key> <value>")
				}
			//Find key in the active ring - get <key>
			} else if commandTokens[0] == "get" {
				if len(commandTokens) == 2 {
					command := Command{}
					command.Address = replica.Cell[0]
					command.Command = strings.Join(commandTokens, " ")
					command.Promise = Sequence{N: 0, Address: replica.Cell[0]}
					command.Tag = rand.Int()
					key := command.Address.IP + "-" + strconv.Itoa(command.Tag)
					command.Key = key

					responseChannel := make(chan string, 1)
					replica.Mutex.RLock()
					replica.Listeners[key] = responseChannel
					replica.Mutex.RUnlock()

					send := ProposeReq{Command: command}
					reply := ProposeResp{}
					RandLatency()
					Call(replica.Cell[0].String(), "Replica.Propose", send, &reply)
					RandLatency()
					fmt.Println(<-replica.Listeners[key])
				} else {
					fmt.Println("Number of arguments supplied incorrect - usage: get <key>")
				}
			//Delete key from the active ring - delete <key>
			} else if commandTokens[0] == "delete" {
				if len(commandTokens) == 2 {
					command := Command{}
					command.Address = replica.Cell[0]
					command.Command = strings.Join(commandTokens, " ")
					command.Promise = Sequence{N: 0, Address: replica.Cell[0]}
					command.Tag = rand.Int()
					key := command.Address.IP + "-" + strconv.Itoa(command.Tag)
					command.Key = key

					responseChannel := make(chan string, 1)
					replica.Mutex.RLock()
					replica.Listeners[key] = responseChannel
					replica.Mutex.RUnlock()
					send := ProposeReq{Command: command}
					reply := ProposeResp{}
					RandLatency()
					Call(replica.Cell[0].String(), "Replica.Propose", send, &reply)
					RandLatency()
					fmt.Println(<-replica.Listeners[key])
				} else {
					fmt.Println("Number of arguments supplied incorrect - usage: delete <key>")
				}
			//Display information about the current node - dump
			} else if commandTokens[0] == "dump" {
				var reply *string
				Call(replica.Cell[0].String(), "Replica.Dump", sendNothing, &reply)
				fmt.Println(*reply)
			//Dump information on all replicas - dumpall
			} else if commandTokens[0] == "dumpall" {
				var reply string
				for _, address := range replica.Cell {
					Call(address.String(), "Replica.Dump", sendNothing, &reply)
					fmt.Println("-----" + address.String() + "-----")
					fmt.Println(reply)
				}
			//Check if node is alive - ping <address>:<port>
			} else if commandTokens[0] == "ping" {
				if len(commandTokens) == 2 {
					var reply *int
					Call(commandTokens[1], "Replica.Ping", sendNothing, &reply)
					if reply != nil && *reply == 562 {
						fmt.Println("Response recieved from " + commandTokens[1])
					} else {
						fmt.Println("No response from " + commandTokens[1])
					}
				} else {
					fmt.Println("Number of arguments supplied incorrect - usage: ping <addr>")
				}
			//List of help commands
			} else if commandTokens[0] == "help" {
				var buffer bytes.Buffer
				buffer.WriteString("\n--- List of Paxos Commands --- \n")
				buffer.WriteString("--- Key/Value Operations --- \n")
				buffer.WriteString("     put <key> <value> : Insert the <key> and <value> into the database\n")
				buffer.WriteString("     get <key>         : Find <key> in the database\n")
				buffer.WriteString("     delete <key>      : Delete <key> from the database\n")
				buffer.WriteString("     quit              : Shut down this replica instance\n")
				buffer.WriteString("--- Debugging Commands ---\n")
				buffer.WriteString("     dump              : Display information about the current replica\n")
				buffer.WriteString("     dumpall           : Display information about all active replicas\n")
				buffer.WriteString("     ping <addr:port>  : Checks to see if replica at address:port is listening\n")

				fmt.Println(buffer.String())
			//Exit program
			} else if commandTokens[0] == "quit" {
				fmt.Println("Quitting...")
				os.Exit(1)
			} else {
				fmt.Println("Command not recognized")
			}
			PrintPrompt()
		}
	}
	//Error handling
	if err := scanner.Err(); err != nil {
		PrintPrompt()
		fmt.Fprintln(os.Stderr, "Reading standard input:", err)
	}
}
