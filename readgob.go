package main

import (
	"bytes"
	"encoding/gob"
	"flag"
	"fmt"
	"log"
	"os"

	"kv/kvstore"
	"kv/raft"
)

func init() {
	// 注册 Op 的真实类型，避免 decode 出错
	gob.Register(kvstore.Op{})
}

func decodePersistentState(file string) {
	data, err := os.ReadFile(file)
	if err != nil {
		log.Fatalf("read file failed: %v", err)
	}

	var state raft.PersistentState
	decoder := gob.NewDecoder(bytes.NewBuffer(data))
	if err := decoder.Decode(&state); err != nil {
		log.Fatalf("decode persistent state failed: %v", err)
	}

	fmt.Printf("CurrentTerm: %d\n", state.CurrentTerm)
	fmt.Printf("VoteFor: %d\n", state.VoteFor)
	fmt.Println("Logs:")
	for _, logEntry := range state.Logs {
		fmt.Printf("  Index=%d Term=%d Command=%#v\n", logEntry.Index, logEntry.Term, logEntry.Command)
	}
}

func decodeSnapshot(file string) {
	data, err := os.ReadFile(file)
	if err != nil {
		log.Fatalf("read file failed: %v", err)
	}

	var kvState map[string]string
	decoder := gob.NewDecoder(bytes.NewBuffer(data))
	if err := decoder.Decode(&kvState); err != nil {
		log.Fatalf("decode snapshot failed: %v", err)
	}

	for k, v := range kvState {
		fmt.Printf("  %s %s\n", k, v)
	}
}

func main() {
	// 定义命令行参数
	filePtr := flag.String("file", "", "Path to the gob file to decode")
	modePtr := flag.String("mode", "", "Decoding mode: 'state' or 'snapshot'")
	flag.Parse()

	if *filePtr == "" || *modePtr == "" {
		fmt.Fprintf(os.Stderr, "Usage: %s -file <filename> -mode <state|snapshot>\n", os.Args[0])
		os.Exit(1)
	}

	switch *modePtr {
	case "state":
		decodePersistentState(*filePtr)
	case "snapshot":
		decodeSnapshot(*filePtr)
	default:
		log.Fatalf("invalid mode: %s, must be 'state' or 'snapshot'", *modePtr)
	}
}
