// Command kvctl is a CLI client for the distributed key-value store.
//
//	kvctl --peers 127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002 put name ada
//	kvctl --peers ... get name
//	kvctl --peers ... append name lovelace
//	kvctl --peers ... delete name
//	kvctl --peers ... status
package main

import (
	"flag"
	"fmt"
	"net/rpc"
	"os"
	"strings"

	"kvraft/kv"
)

func usage() {
	fmt.Fprintln(os.Stderr, "usage: kvctl --peers h:p,h:p,... <get KEY | put KEY VAL | append KEY VAL | delete KEY | status>")
	os.Exit(2)
}

func main() {
	peersCSV := flag.String("peers", "127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002",
		"comma-separated host:port of every server")
	flag.Parse()
	peers := strings.Split(*peersCSV, ",")
	args := flag.Args()
	if len(args) == 0 {
		usage()
	}

	switch args[0] {
	case "status":
		printStatus(peers)
		return
	}

	ck := kv.MakeClerk(peers)
	switch args[0] {
	case "get":
		if len(args) != 2 {
			usage()
		}
		fmt.Println(ck.Get(args[1]))
	case "put":
		if len(args) != 3 {
			usage()
		}
		ck.Put(args[1], args[2])
		fmt.Println("OK")
	case "append":
		if len(args) != 3 {
			usage()
		}
		ck.Append(args[1], args[2])
		fmt.Println("OK")
	case "delete":
		if len(args) != 2 {
			usage()
		}
		ck.Delete(args[1])
		fmt.Println("OK")
	default:
		usage()
	}
}

// printStatus queries every server directly and shows who the leader is.
func printStatus(peers []string) {
	fmt.Printf("%-3s %-22s %-8s %-6s %-8s %s\n", "id", "address", "term", "leader", "applied", "keys")
	for i, addr := range peers {
		c, err := rpc.Dial("tcp", addr)
		if err != nil {
			fmt.Printf("%-3d %-22s %s\n", i, addr, "DOWN")
			continue
		}
		reply := &kv.StatusReply{}
		err = c.Call("KV.Status", &kv.StatusArgs{}, reply)
		_ = c.Close()
		if err != nil {
			fmt.Printf("%-3d %-22s %s\n", i, addr, "ERR")
			continue
		}
		leader := "no"
		if reply.IsLeader {
			leader = "YES"
		}
		fmt.Printf("%-3d %-22s %-8d %-6s %-8d %d\n", i, addr, reply.Term, leader, reply.Applied, reply.Keys)
	}
}
