package server

import (
	"bufio"
	"kv/pkg/kvstore"
	"kv/pkg/raft"
	"net"
	"strings"
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
			op := kvstore.Op{Type: "Put", Key: parts[1], Value: parts[2]}
			_, _, isLeader := rf.Start(op)
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
			op := kvstore.Op{Type: "Del", Key: parts[1]}
			_, _, isLeader := rf.Start(op)
			if isLeader {
				conn.Write([]byte("OK\n"))
			} else {
				conn.Write([]byte("Not_Leader\n"))
			}

		default:
			conn.Write([]byte("ERR Unknown command\n"))
		}
	}
}
