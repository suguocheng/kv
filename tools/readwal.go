package main

import (
	"encoding/gob"
	"fmt"
	"io"
	"kv/pkg/kvpb"
	"kv/pkg/kvstore" // 修改为你的实际包路径
	"os"

	"google.golang.org/protobuf/proto"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("用法: wal_dump <wal文件路径>")
		return
	}
	walPath := os.Args[1]
	file, err := os.Open(walPath)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	dec := gob.NewDecoder(file)
	entryCount := 0
	decodeErrCount := 0
	for {
		var entry kvstore.WALEntry
		err := dec.Decode(&entry)
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Printf("[DecodeError] entry %d: %v\n", entryCount+1, err)
			decodeErrCount++
			continue
		}
		entryCount++
		fmt.Printf("---- Entry #%d ----\n", entryCount)
		fmt.Printf("Term=%d Index=%d Type=%d\n", entry.Term, entry.Index, entry.Type)
		// 如果Data是protobuf序列化的Op，可以进一步反序列化
		var op kvpb.Op
		if err := proto.Unmarshal(entry.Data, &op); err == nil {
			fmt.Printf("  Op.Type=%s Key=%s Value=%s Ttl=%d\n", op.Type, op.Key, string(op.Value), op.Ttl)
		} else {
			// 打印Data前16字节的hex
			max := 16
			if len(entry.Data) < max {
				max = len(entry.Data)
			}
			fmt.Printf("  Data(raw, hex): %x", entry.Data[:max])
			if len(entry.Data) > max {
				fmt.Printf(" ... (total %d bytes)", len(entry.Data))
			}
			fmt.Println()
		}
	}
	fmt.Printf("\n共读取entry: %d, 解码失败: %d\n", entryCount, decodeErrCount)
}
