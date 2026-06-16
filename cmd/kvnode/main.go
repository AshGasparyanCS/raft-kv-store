// Command kvnode runs a single replica of the distributed key-value store.
//
// Example 3-node cluster (run each in its own terminal):
//
//	kvnode --id 0 --peers 127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002 --data ./data0
//	kvnode --id 1 --peers 127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002 --data ./data1
//	kvnode --id 2 --peers 127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002 --data ./data2
package main

import (
	"flag"
	"log"
	"net"
	"net/rpc"
	"strings"

	"kvraft/kv"
	"kvraft/raft"
)

func main() {
	id := flag.Int("id", 0, "this server's id (index into --peers)")
	peersCSV := flag.String("peers", "127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002",
		"comma-separated host:port of every server, ordered by id")
	listen := flag.String("listen", "", "address to listen on (defaults to this server's --peers entry)")
	dataDir := flag.String("data", "./data", "directory for persisted raft state and snapshots")
	maxState := flag.Int("maxstate", 8192, "snapshot when persisted raft state exceeds this many bytes (-1 = never)")
	flag.Parse()

	peers := strings.Split(*peersCSV, ",")
	if *id < 0 || *id >= len(peers) {
		log.Fatalf("--id %d out of range for %d peers", *id, len(peers))
	}
	addr := *listen
	if addr == "" {
		addr = peers[*id]
	}

	persister, err := raft.NewFilePersister(*dataDir)
	if err != nil {
		log.Fatalf("persister: %v", err)
	}

	peerIDs := make([]int, len(peers))
	for i := range peerIDs {
		peerIDs[i] = i
	}
	transport := raft.NewNetTransport(peers)
	server := kv.StartKVServer(peerIDs, *id, transport, persister, *maxState)

	// Expose both the inter-node Raft RPCs and the client KV RPCs on one port.
	rpcServer := rpc.NewServer()
	if err := rpcServer.RegisterName("RaftRPC", raft.NewRPCEndpoint(server.Raft())); err != nil {
		log.Fatalf("register RaftRPC: %v", err)
	}
	if err := rpcServer.RegisterName("KV", server); err != nil {
		log.Fatalf("register KV: %v", err)
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("listen %s: %v", addr, err)
	}
	log.Printf("kvnode %d listening on %s (cluster of %d, snapshot threshold %d bytes)",
		*id, addr, len(peers), *maxState)

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("accept: %v", err)
			continue
		}
		go rpcServer.ServeConn(conn)
	}
}
