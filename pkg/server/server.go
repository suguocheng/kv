package server

import (
	"bufio"
	"encoding/base64"
	"kv/pkg/kvpb"
	"kv/pkg/kvstore"
	"kv/pkg/raft"
	"net"
	"strconv"
	"strings"

	"google.golang.org/protobuf/proto"
)

func HandleConnection(conn net.Conn, kv *kvstore.KV, rf *raft.Raft) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		parts := strings.Fields(strings.TrimSpace(line))
		if len(parts) == 0 {
			continue
		}

		cmd := strings.ToUpper(parts[0])

		switch cmd {
		case "GET":
			if len(parts) != 2 {
				conn.Write([]byte("ERR Usage: GET key\n"))
				continue
			}
			val, ok := kv.Get(parts[1])
			if ok == nil {
				conn.Write([]byte(val + "\n"))
			} else {
				conn.Write([]byte("NOTFOUND\n"))
			}

		case "PUT":
			if len(parts) != 3 {
				conn.Write([]byte("ERR Usage: PUT key value\n"))
				continue
			}
			op := &kvpb.Op{Type: "Put", Key: parts[1], Value: parts[2], Ttl: 0}
			data, err := proto.Marshal(op)
			if err != nil {
				conn.Write([]byte("ERR marshal failed\n"))
				continue
			}
			_, _, isLeader := rf.Start(data)
			if isLeader {
				conn.Write([]byte("OK\n"))
			} else {
				conn.Write([]byte("Not_Leader\n"))
			}

		case "PUTTTL":
			if len(parts) != 4 {
				conn.Write([]byte("ERR Usage: PUTTTL key value ttl\n"))
				continue
			}
			ttl, err := strconv.ParseInt(parts[3], 10, 64)
			if err != nil || ttl < 0 {
				conn.Write([]byte("ERR Invalid TTL (must be non-negative integer)\n"))
				continue
			}
			op := &kvpb.Op{Type: "PutTTL", Key: parts[1], Value: parts[2], Ttl: ttl}
			data, err := proto.Marshal(op)
			if err != nil {
				conn.Write([]byte("ERR marshal failed\n"))
				continue
			}
			_, _, isLeader := rf.Start(data)
			if isLeader {
				conn.Write([]byte("OK\n"))
			} else {
				conn.Write([]byte("Not_Leader\n"))
			}

		case "DEL":
			if len(parts) != 2 {
				conn.Write([]byte("ERR Usage: DEL key\n"))
				continue
			}
			op := &kvpb.Op{Type: "Del", Key: parts[1]}
			data, err := proto.Marshal(op)
			if err != nil {
				conn.Write([]byte("ERR marshal failed\n"))
				continue
			}
			_, _, isLeader := rf.Start(data)
			if isLeader {
				conn.Write([]byte("OK\n"))
			} else {
				conn.Write([]byte("Not_Leader\n"))
			}

		default:
			if cmd == "TXN" && len(parts) == 2 {
				// parts[1]为base64编码的TxnRequest
				data, err := base64.StdEncoding.DecodeString(parts[1])
				if err != nil {
					conn.Write([]byte("ERR txn base64 decode failed\n"))
					continue
				}
				var req kvpb.TxnRequest
				if err := proto.Unmarshal(data, &req); err != nil {
					conn.Write([]byte("ERR txn proto unmarshal failed\n"))
					continue
				}
				resp, err := kv.Txn(&req)
				if err != nil {
					conn.Write([]byte("ERR txn failed\n"))
					continue
				}
				respData, err := proto.Marshal(resp)
				if err != nil {
					conn.Write([]byte("ERR txn marshal failed\n"))
					continue
				}
				b64 := base64.StdEncoding.EncodeToString(respData)
				conn.Write([]byte(b64 + "\n"))
			} else {
				conn.Write([]byte("ERR Unknown command\n"))
			}
		}
	}
}
