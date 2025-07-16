package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"kv/pkg/proto/kvpb"
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

	entryCount := 0
	decodeErrCount := 0
	for {
		var lenBuf [4]byte
		_, err := file.Read(lenBuf[:])
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Printf("[ReadError] entry %d: %v\n", entryCount+1, err)
			decodeErrCount++
			continue
		}
		msgLen := binary.BigEndian.Uint32(lenBuf[:])
		msgBuf := make([]byte, msgLen)
		_, err = file.Read(msgBuf)
		if err != nil {
			fmt.Printf("[ReadError] entry %d: %v\n", entryCount+1, err)
			decodeErrCount++
			continue
		}
		var entry kvpb.WALEntry
		if err := proto.Unmarshal(msgBuf, &entry); err != nil {
			fmt.Printf("[DecodeError] entry %d: %v\n", entryCount+1, err)
			decodeErrCount++
			continue
		}
		entryCount++
		fmt.Printf("---- Entry #%d ----\n", entryCount)
		fmt.Printf("Term=%d Index=%d Type=%v\n", entry.Term, entry.Index, entry.Type)
		// 如果Data是protobuf序列化的Op，可以进一步反序列化
		var op kvpb.Op
		if err := proto.Unmarshal(entry.Data, &op); err == nil {
			fmt.Printf("  Op.Type=%s Key=%s Value=%s Ttl=%d\n", op.Type, op.Key, string(op.Value), op.Ttl)
			if op.Type == "Txn" {
				var req kvpb.TxnRequest
				if err := proto.Unmarshal(op.Value, &req); err == nil {
					fmt.Printf("    [TxnRequest] Success ops count: %d\n", len(req.Success))
					for i, sop := range req.Success {
						fmt.Printf("      Success[%d]: Type=%s Key=%s Value=%s Ttl=%d\n", i, sop.Type, sop.Key, string(sop.Value), sop.Ttl)
					}
					fmt.Printf("    [TxnRequest] Failure ops count: %d\n", len(req.Failure))
					for i, fop := range req.Failure {
						fmt.Printf("      Failure[%d]: Type=%s Key=%s Value=%s Ttl=%d\n", i, fop.Type, fop.Key, string(fop.Value), fop.Ttl)
					}
				} else {
					fmt.Printf("    [TxnRequest] proto.Unmarshal失败: %v\n", err)
				}
			}
		} else {
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
