package main

import (
	"fmt"
	"kv/kvstore"
	"kv/raft"
	"kv/server"
	"net"
	"os"
	"strconv"
)

func main() {
	//配置
	args := os.Args
	if len(args) != 2 {
		fmt.Println("Usage: go run main.go <nodeID>")
		os.Exit(1)
	}
	me, _ := strconv.Atoi(args[1]) //当前节点 ID

	peerAddrs := map[int]string{
		0: "localhost:8000",
		1: "localhost:8001",
		2: "localhost:8002",
	}

	clientPort := 9000 + me
	myAddr := fmt.Sprintf("localhost:%d", clientPort)
	raftstatePath := fmt.Sprintf("data/node%d/raft-state.gob", me)
	raftsnapshotPath := fmt.Sprintf("data/node%d/snapshot.gob", me)
	kvLogPath := fmt.Sprintf("data/node%d/command.log", me)

	//启动 applyCh 和模块
	applyCh := make(chan raft.ApplyMsg)
	rf := raft.Make(me, peerAddrs, myAddr, applyCh, raftstatePath, raftsnapshotPath)
	kv, err := kvstore.NewKV(kvLogPath)
	if err != nil {
		panic(err)
	}
	defer kv.Close()

	lastSnapshottedIndex := 0

	//监听 applyCh：Raft 已提交日志应用到 KV
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

	//启动 TCP 服务器，监听客户端连接
	ln, err := net.Listen("tcp", myAddr)
	if err != nil {
		panic(err)
	}
	fmt.Println("Node", me, "listening on", myAddr)

	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}
		go server.HandleConnection(conn, kv, rf)
	}
}
