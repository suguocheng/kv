package server

import (
	"bufio"
	"encoding/base64"
	"fmt"
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
			val, err := kv.Get(parts[1])
			if err == nil {
				conn.Write([]byte(val + "\n"))
			} else {
				conn.Write([]byte("NOTFOUND\n"))
			}

		case "GETREV":
			if len(parts) != 3 {
				conn.Write([]byte("ERR Usage: GETREV key revision\n"))
				continue
			}
			revision, err := strconv.ParseInt(parts[2], 10, 64)
			if err != nil {
				conn.Write([]byte("ERR Invalid revision number\n"))
				continue
			}
			val, rev, err := kv.GetWithRevision(parts[1], revision)
			if err == nil {
				conn.Write([]byte(fmt.Sprintf("%d %s\n", rev, val)))
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

		case "HISTORY":
			if len(parts) != 2 {
				conn.Write([]byte("ERR Usage: HISTORY <base64_encoded_request>\n"))
				continue
			}
			handleHistoryRequest(conn, parts[1], kv)

		case "RANGE":
			if len(parts) != 2 {
				conn.Write([]byte("ERR Usage: RANGE <base64_encoded_request>\n"))
				continue
			}
			handleRangeRequest(conn, parts[1], kv)

		case "COMPACT":
			if len(parts) != 2 {
				conn.Write([]byte("ERR Usage: COMPACT <base64_encoded_request>\n"))
				continue
			}
			handleCompactRequest(conn, parts[1], kv, rf)

		case "STATS":
			handleStatsRequest(conn, kv)

		case "TXN":
			if len(parts) != 2 {
				conn.Write([]byte("ERR Usage: TXN <base64_encoded_request>\n"))
				continue
			}
			handleTxnRequest(conn, parts[1], kv)

		default:
			conn.Write([]byte("ERR Unknown command\n"))
		}
	}
}

// handleHistoryRequest 处理历史查询请求
func handleHistoryRequest(conn net.Conn, b64Data string, kv *kvstore.KV) {
	data, err := base64.StdEncoding.DecodeString(b64Data)
	if err != nil {
		conn.Write([]byte("ERR history base64 decode failed\n"))
		return
	}

	var req kvpb.HistoryRequest
	if err := proto.Unmarshal(data, &req); err != nil {
		conn.Write([]byte("ERR history proto unmarshal failed\n"))
		return
	}

	history, err := kv.GetHistory(req.Key, req.Limit)
	if err != nil {
		conn.Write([]byte("ERR history query failed\n"))
		return
	}

	// 转换为protobuf格式
	var resp kvpb.HistoryResponse
	for _, version := range history {
		resp.History = append(resp.History, &kvpb.VersionedKVPair{
			Key:             version.Key,
			Value:           version.Value,
			Ttl:             version.TTL,
			CreatedRevision: version.CreatedRev,
			ModRevision:     version.ModRev,
			Version:         version.Version,
			Deleted:         version.Deleted,
		})
	}

	respData, err := proto.Marshal(&resp)
	if err != nil {
		conn.Write([]byte("ERR history marshal failed\n"))
		return
	}

	b64 := base64.StdEncoding.EncodeToString(respData)
	conn.Write([]byte(b64 + "\n"))
}

// handleRangeRequest 处理范围查询请求
func handleRangeRequest(conn net.Conn, b64Data string, kv *kvstore.KV) {
	data, err := base64.StdEncoding.DecodeString(b64Data)
	if err != nil {
		conn.Write([]byte("ERR range base64 decode failed\n"))
		return
	}

	var req kvpb.RangeRequest
	if err := proto.Unmarshal(data, &req); err != nil {
		conn.Write([]byte("ERR range proto unmarshal failed\n"))
		return
	}

	results, revision, err := kv.Range(req.Key, req.RangeEnd, req.Revision, req.Limit)
	if err != nil {
		conn.Write([]byte("ERR range query failed\n"))
		return
	}

	// 转换为protobuf格式
	var resp kvpb.RangeResponse
	resp.Revision = revision
	for _, result := range results {
		resp.Kvs = append(resp.Kvs, &kvpb.VersionedKVPair{
			Key:             result.Key,
			Value:           result.Value,
			Ttl:             result.TTL,
			CreatedRevision: result.CreatedRev,
			ModRevision:     result.ModRev,
			Version:         result.Version,
			Deleted:         result.Deleted,
		})
	}

	respData, err := proto.Marshal(&resp)
	if err != nil {
		conn.Write([]byte("ERR range marshal failed\n"))
		return
	}

	b64 := base64.StdEncoding.EncodeToString(respData)
	conn.Write([]byte(b64 + "\n"))
}

// handleCompactRequest 处理压缩请求
func handleCompactRequest(conn net.Conn, b64Data string, kv *kvstore.KV, rf *raft.Raft) {
	data, err := base64.StdEncoding.DecodeString(b64Data)
	if err != nil {
		conn.Write([]byte("ERR compact base64 decode failed\n"))
		return
	}

	var req kvpb.CompactRequest
	if err := proto.Unmarshal(data, &req); err != nil {
		conn.Write([]byte("ERR compact proto unmarshal failed\n"))
		return
	}

	// 压缩操作需要通过Raft共识
	op := &kvpb.Op{
		Type:  "Compact",
		Key:   fmt.Sprintf("compact_%d", req.Revision), // 使用key字段存储版本号
		Value: fmt.Sprintf("%d", req.Revision),
	}
	opData, err := proto.Marshal(op)
	if err != nil {
		conn.Write([]byte("ERR compact marshal failed\n"))
		return
	}

	_, _, isLeader := rf.Start(opData)
	if !isLeader {
		conn.Write([]byte("Not_Leader\n"))
		return
	}

	// 对于读操作，直接返回结果
	compactedRev, err := kv.Compact(req.Revision)
	if err != nil {
		conn.Write([]byte("ERR compact failed\n"))
		return
	}

	var resp kvpb.CompactResponse
	resp.Revision = compactedRev

	respData, err := proto.Marshal(&resp)
	if err != nil {
		conn.Write([]byte("ERR compact response marshal failed\n"))
		return
	}

	b64 := base64.StdEncoding.EncodeToString(respData)
	conn.Write([]byte(b64 + "\n"))
}

// handleStatsRequest 处理统计信息请求
func handleStatsRequest(conn net.Conn, kv *kvstore.KV) {
	stats := kv.GetStats()

	// 格式化统计信息为字符串
	var parts []string
	for key, value := range stats {
		parts = append(parts, fmt.Sprintf("%v", key), fmt.Sprintf("%v", value))
	}

	response := strings.Join(parts, " ")
	conn.Write([]byte(response + "\n"))
}

// handleTxnRequest 处理事务请求
func handleTxnRequest(conn net.Conn, b64Data string, kv *kvstore.KV) {
	data, err := base64.StdEncoding.DecodeString(b64Data)
	if err != nil {
		conn.Write([]byte("ERR txn base64 decode failed\n"))
		return
	}
	var req kvpb.TxnRequest
	if err := proto.Unmarshal(data, &req); err != nil {
		conn.Write([]byte("ERR txn proto unmarshal failed\n"))
		return
	}
	resp, err := kv.Txn(&req)
	if err != nil {
		conn.Write([]byte("ERR txn failed\n"))
		return
	}
	respData, err := proto.Marshal(resp)
	if err != nil {
		conn.Write([]byte("ERR txn marshal failed\n"))
		return
	}
	b64 := base64.StdEncoding.EncodeToString(respData)
	conn.Write([]byte(b64 + "\n"))
}
