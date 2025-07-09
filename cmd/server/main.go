package main

import (
	"fmt"
	"kv/log"
	"kv/pkg/kvpb"
	"kv/pkg/kvstore"
	"kv/pkg/raft"
	"kv/pkg/server"
	"net"
	"os"
	"strconv"

	"google.golang.org/protobuf/proto"
)

type NodeConfig struct {
	Me            int
	ClientAddr    string
	PeerAddrs     map[int]string
	RaftStatePath string
	SnapshotPath  string
	WALDir        string
	MaxWALEntries int // 每个WAL文件最大条目数
}

func main() {
	me := parseNodeID()
	conf := loadNodeConfig(me)

	kv := initKV(conf.WALDir, conf.MaxWALEntries)
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
		RaftStatePath: fmt.Sprintf("data/node%d/raft-state.pb", me),
		SnapshotPath:  fmt.Sprintf("data/node%d/snapshot.pb", me),
		WALDir:        fmt.Sprintf("data/node%d/wal", me),
		MaxWALEntries: 100, // 每个WAL文件最多100个条目，与快照阈值保持一致
	}
}

func initKV(walDir string, maxEntries int) *kvstore.KV {
	kv, err := kvstore.NewKV(walDir, maxEntries)
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
				var op kvpb.Op
				err := proto.Unmarshal(msg.Command, &op)
				if err != nil {
					fmt.Println("Failed to decode command:", err)
					continue
				}

				switch op.Type {
				case "Put":
					kv.Put(op.Key, op.Value)
				case "PutTTL":
					kv.PutWithTTL(op.Key, op.Value, op.Ttl)
				case "Del":
					kv.Delete(op.Key)
				default:
					fmt.Println("Unknown Op type:", op.Type)
				}

				// 快照阈值与MaxWALEntries保持一致，方便删除整个WAL文件
				// 只有当累积的条目数达到阈值时才触发快照
				if msg.CommandIndex-lastSnapshottedIndex >= 100 {
					snapshot, _ := kv.SerializeState()
					rf.Snapshot(msg.CommandIndex, snapshot)
					lastSnapshottedIndex = msg.CommandIndex

					// 清理已快照的WAL文件
					if err := kv.CleanupWALFiles(msg.CommandIndex); err != nil {
						log.DPrintf("Failed to cleanup WAL files: %v\n", err)
					} else {
						log.DPrintf("Cleaned up WAL files up to index %d\n", msg.CommandIndex)
					}
				}
			} else if msg.SnapshotValid {
				// 处理快照安装
				ok := rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot)
				if ok {
					err := kv.RestoreFromSnapshotData(msg.Snapshot)
					if err != nil {
						log.DPrintf("Failed to restore state from snapshot: %v\n", err)
					} else {
						log.DPrintf("Restored state from snapshot at index %d\n", msg.SnapshotIndex)
					}
					lastSnapshottedIndex = msg.SnapshotIndex
				} else {
					log.DPrintf("CondInstallSnapshot rejected snapshot at index %d\n", msg.SnapshotIndex)
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
