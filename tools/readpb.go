package main

// import (
// 	"flag"
// 	"fmt"
// 	"log"
// 	"os"

// 	"google.golang.org/protobuf/encoding/prototext"
// 	"google.golang.org/protobuf/proto"

// 	"kv/pkg/kvpb"
// 	"kv/pkg/raftpb"
// )

// func main() {
// 	var filename string
// 	flag.StringVar(&filename, "file", "", "path to protobuf binary file")
// 	flag.Parse()

// 	if filename == "" {
// 		fmt.Println("Usage: go run main.go -file=yourfile.bin")
// 		os.Exit(1)
// 	}

// 	data, err := os.ReadFile(filename)
// 	if err != nil {
// 		log.Fatalf("Failed to read file: %v", err)
// 	}

// 	// 1. 优先尝试解析为 KVStore
// 	kvstore := &kvpb.KVStore{}
// 	if err := proto.Unmarshal(data, kvstore); err == nil && len(kvstore.Pairs) > 0 {
// 		fmt.Println("Decoded as KVStore:")
// 		fmt.Println(prototext.Format(kvstore))
// 		return
// 	}

// 	// 2. 再尝试解析为 PersistentState，并检查是否非空
// 	ps := &raftpb.PersistentState{}
// 	if err := proto.Unmarshal(data, ps); err == nil && (ps.CurrentTerm != 0 || ps.VoteFor != 0 || len(ps.Logs) > 0) {
// 		fmt.Println("Decoded as PersistentState:")
// 		fmt.Println(prototext.Format(ps))
// 		return
// 	}

// 	log.Fatal("Failed to decode data as either KVStore or PersistentState (or both are empty)")
// }
