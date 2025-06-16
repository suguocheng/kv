package main

import (
	"fmt"
	"kv/pkg/kvstore"
	"kv/pkg/raft"
	"kv/pkg/server"
	"net"
	"os"
	"strconv"
)

type NodeConfig struct {
	Me            int
	ClientAddr    string
	PeerAddrs     map[int]string
	RaftStatePath string
	SnapshotPath  string
	KVLogPath     string
}

func main() {
	me := parseNodeID()
	conf := loadNodeConfig(me)

	kv := initKV(conf.KVLogPath)
	defer kv.Close()

	rf, applyCh := initRaft(conf, kv)

	startApplyLoop(rf, kv, applyCh)

	startClientListener(conf.ClientAddr, kv, rf)
}

func parseNodeID() int {
	if len(os.Args) != 2 {
		fmt.Println("Usage: go run main.go <nodeID>")
		os.Exit(1)
	}
	id, err := strconv.Atoi(os.Args[1])
	if err != nil {
		panic("Invalid node ID")
	}
	return id
}

func loadNodeConfig(me int) *NodeConfig {
	peerAddrs := map[int]string{
		0: "localhost:8000",
		1: "localhost:8001",
		2: "localhost:8002",
	}
	clientPort := 9000 + me

	return &NodeConfig{
		Me:            me,
		ClientAddr:    fmt.Sprintf("localhost:%d", clientPort),
		PeerAddrs:     peerAddrs,
		RaftStatePath: fmt.Sprintf("data/node%d/raft-state.gob", me),
		SnapshotPath:  fmt.Sprintf("data/node%d/snapshot.gob", me),
		KVLogPath:     fmt.Sprintf("data/node%d/command.log", me),
	}
}

func initKV(path string) *kvstore.KV {
	kv, err := kvstore.NewKV(path)
	if err != nil {
		panic(err)
	}
	return kv
}

func initRaft(conf *NodeConfig, kv *kvstore.KV) (*raft.Raft, chan raft.ApplyMsg) {
	applyCh := make(chan raft.ApplyMsg)
	rf := raft.Make(conf.Me, conf.PeerAddrs, conf.ClientAddr, applyCh, conf.RaftStatePath, conf.SnapshotPath)

	go startApplyLoop(rf, kv, applyCh)

	return rf, applyCh
}

func startApplyLoop(rf *raft.Raft, kv *kvstore.KV, applyCh chan raft.ApplyMsg) {
	lastSnapshottedIndex := 0
	go func() {
		for msg := range applyCh {
			if msg.CommandValid {
				op, ok := msg.Command.(kvstore.Op)
				if !ok {
					fmt.Println("Invalid command in applyCh")
					continue
				}
				switch op.Type {
				case "Put":
					kv.Put(op.Key, op.Value)
				case "Del":
					kv.Delete(op.Key)
				}
				if msg.CommandIndex-lastSnapshottedIndex >= 5 {
					snapshot := kv.SerializeState()
					rf.Snapshot(msg.CommandIndex, snapshot)
					lastSnapshottedIndex = msg.CommandIndex
				}
			}
		}
	}()
}

func startClientListener(addr string, kv *kvstore.KV, rf *raft.Raft) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		panic(err)
	}
	fmt.Println("Listening on", addr)
	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}
		go server.HandleConnection(conn, kv, rf)
	}
}
