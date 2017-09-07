package main

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"time"
)

//Standalone functions that are not part of the replica

//Get IP address
func GetLocalAddress() string {
	var localaddress string
	ifaces, err := net.Interfaces()
	if err != nil {
		panic("init: failed to find network interfaces")
	}
	//Find the first non-loopback interface with an IP address
	for _, elt := range ifaces {
		if elt.Flags&net.FlagLoopback == 0 && elt.Flags&net.FlagUp != 0 {
			addrs, err := elt.Addrs()
			if err != nil {
				panic("init: failed to get addresses for network interface")
			}
			for _, addr := range addrs {
				if ipnet, ok := addr.(*net.IPNet); ok {
					if ip4 := ipnet.IP.To4(); len(ip4) == net.IPv4len {
						localaddress = ip4.String()
						break
					}
				}
			}
		}
	}
	if localaddress == "" {
		panic("init: failed to find non-loopback interface with valid address on this node")
	}
	return localaddress
}

//Make an RPC call at 'address' with name 'method' and load results of call into 'reply'
func Call(address string, method string, request interface{}, reply interface{}) error {
	client, err := rpc.DialHTTP("tcp", address)
	if err != nil {
		return err
	}

	if err = client.Call(method, request, reply); err != nil {
		log.Fatalf("Error calling "+method+": %v", err)
	}
	client.Close()
	return err
}

func PrintPrompt(args ...string) {
	prefix := "paxos> "
	if len(args) == 0 {
		fmt.Printf(prefix)
	} else if len(args) == 1 {
		fmt.Printf(prefix + " " + args[0] + "\n")
	} else {
		fmt.Printf(prefix+" "+args[0]+"\n", args[1:])
	}
}

//Returns a random value between latency and 2*latency
func CalcRandLatency(lat int) int {
	return lat + rand.Intn(lat+1)%(2*lat)
}

//Sleep for a random amount of time
func RandLatency(args ...int) {
	if args == nil {
		if *latency > 0 {
			time.Sleep(time.Duration(CalcRandLatency(*latency)) * time.Millisecond)
		}
	} else {
		time.Sleep(time.Duration(CalcRandLatency(args[0])) * time.Millisecond)
	}
}

func chatf(level int, format string, args ...interface{}) {
	if *chatty >= level {
		fmt.Printf(format+"\n", args...)
	}
}

func everySlotDecided(slots []Slot, n int) bool {
	if n == 1 {
		return true
	}
	for key, value := range slots {
		if key == 0 {
			continue
		}
		if key >= n {
			break
		}
		if !value.Decided {
			return false
		}
	}
	return true
}
