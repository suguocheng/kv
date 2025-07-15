package server

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"kv/pkg/kvpb"
	"kv/pkg/kvstore"
	"kv/pkg/raft"
	"kv/pkg/watch"
	"net"
	"strconv"
	"strings"
	"time"

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
			handleGetRequest(conn, parts[1], kv)

		case "GETREV":
			if len(parts) != 3 {
				conn.Write([]byte("ERR Usage: GETREV key revision\n"))
				continue
			}
			handleGetRevRequest(conn, parts[1], parts[2], kv)

		case "PUT":
			if len(parts) != 3 {
				conn.Write([]byte("ERR Usage: PUT key value\n"))
				continue
			}
			handlePutRequest(conn, parts[1], parts[2], rf)

		case "PUTTTL":
			if len(parts) != 4 {
				conn.Write([]byte("ERR Usage: PUTTTL key value ttl\n"))
				continue
			}
			handlePutTTLRequest(conn, parts[1], parts[2], parts[3], rf)

		case "DEL":
			if len(parts) != 2 {
				conn.Write([]byte("ERR Usage: DEL key\n"))
				continue
			}
			handleDelRequest(conn, parts[1], rf)

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
			handleTxnRequest(conn, parts[1], kv, rf)

		case "WATCH":
			if len(parts) != 2 {
				conn.Write([]byte("ERR Usage: WATCH <base64_encoded_request>\n"))
				continue
			}
			handleWatchRequest(conn, parts[1], kv)

		case "UNWATCH":
			if len(parts) != 2 {
				conn.Write([]byte("ERR Usage: UNWATCH <watcher_id>\n"))
				continue
			}
			handleUnwatchRequest(conn, parts[1], kv)

		case "WATCHSTATS":
			handleWatchStatsRequest(conn, kv)

		default:
			conn.Write([]byte("ERR Unknown command\n"))
		}
	}
}

// handleGetRequest 处理GET请求
func handleGetRequest(conn net.Conn, key string, kv *kvstore.KV) {
	val, err := kv.Get(key)
	if err == nil {
		conn.Write([]byte(val + "\n"))
	} else {
		conn.Write([]byte("NOTFOUND\n"))
	}
}

// handleGetRevRequest 处理版本GET请求
func handleGetRevRequest(conn net.Conn, key, revStr string, kv *kvstore.KV) {
	revision, err := strconv.ParseInt(revStr, 10, 64)
	if err != nil {
		conn.Write([]byte("ERR Invalid revision number\n"))
		return
	}

	val, rev, err := kv.GetWithRevision(key, revision)
	if err == nil {
		conn.Write([]byte(fmt.Sprintf("%d %s\n", rev, val)))
	} else {
		conn.Write([]byte("NOTFOUND\n"))
	}
}

// handlePutRequest 处理PUT请求
func handlePutRequest(conn net.Conn, key, value string, rf *raft.Raft) {
	op := &kvpb.Op{Type: "Put", Key: key, Value: []byte(value), Ttl: 0}
	data, err := proto.Marshal(op)
	if err != nil {
		conn.Write([]byte("ERR marshal failed\n"))
		return
	}
	_, _, isLeader := rf.Start(data)
	if isLeader {
		conn.Write([]byte("OK\n"))
	} else {
		conn.Write([]byte("Not_Leader\n"))
	}
}

// handlePutTTLRequest 处理PUTTTL请求
func handlePutTTLRequest(conn net.Conn, key, value, ttlStr string, rf *raft.Raft) {
	ttl, err := strconv.ParseInt(ttlStr, 10, 64)
	if err != nil || ttl < 0 {
		conn.Write([]byte("ERR Invalid TTL (must be non-negative integer)\n"))
		return
	}

	op := &kvpb.Op{Type: "PutTTL", Key: key, Value: []byte(value), Ttl: ttl}
	data, err := proto.Marshal(op)
	if err != nil {
		conn.Write([]byte("ERR marshal failed\n"))
		return
	}
	_, _, isLeader := rf.Start(data)
	if isLeader {
		conn.Write([]byte("OK\n"))
	} else {
		conn.Write([]byte("Not_Leader\n"))
	}
}

func handleDelRequest(conn net.Conn, key string, rf *raft.Raft) {
	op := &kvpb.Op{Type: "Del", Key: key}
	data, err := proto.Marshal(op)
	if err != nil {
		conn.Write([]byte("ERR marshal failed\n"))
		return
	}
	_, _, isLeader := rf.Start(data)
	if isLeader {
		conn.Write([]byte("OK\n"))
	} else {
		conn.Write([]byte("Not_Leader\n"))
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
		Value: []byte(fmt.Sprintf("%d", req.Revision)),
	}
	opData, err := proto.Marshal(op)
	if err != nil {
		conn.Write([]byte("ERR compact marshal failed\n"))
		return
	}

	_, _, isLeader := rf.Start(opData)
	if isLeader {
		conn.Write([]byte("OK\n"))
	} else {
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
func handleTxnRequest(conn net.Conn, b64Data string, kv *kvstore.KV, rf *raft.Raft) {
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

	// 检查是否为leader
	_, isLeader := rf.GetState()
	if !isLeader {
		conn.Write([]byte("Not_Leader\n"))
		return
	}

	// 将事务请求包装为Op，通过raft层处理
	txnBytes, _ := proto.Marshal(&req)
	op := &kvpb.Op{
		Type:  "Txn",
		Key:   "txn",    // 使用固定的key标识事务操作
		Value: txnBytes, // 直接存储protobuf序列化内容
	}
	opData, err := proto.Marshal(op)
	if err != nil {
		conn.Write([]byte("ERR txn marshal failed\n"))
		return
	}

	// 提交到raft层并获取索引
	index, _, _ := rf.Start(opData)

	// 等待raft应用完成
	// 使用raft层的同步等待机制
	if !rf.WaitForIndex(index, 5*time.Second) {
		conn.Write([]byte("ERR txn timeout\n"))
		return
	}

	// 检查是否仍然是leader
	_, stillLeader := rf.GetState()
	if !stillLeader {
		conn.Write([]byte("Not_Leader\n"))
		return
	}

	// 事务已经在raft层执行完成，现在获取结果
	// 直接执行事务获取结果（raft层已经执行过了，这里只是获取响应）
	resp, err := kv.TxnWithoutWAL(&req)
	if err != nil {
		conn.Write([]byte("ERR txn failed: " + err.Error() + "\n"))
		return
	}

	// 返回事务响应
	respData, err := proto.Marshal(resp)
	if err != nil {
		conn.Write([]byte("ERR txn response marshal failed\n"))
		return
	}

	b64 := base64.StdEncoding.EncodeToString(respData)
	conn.Write([]byte(b64 + "\n"))
}

// handleWatchRequest 处理Watch请求
func handleWatchRequest(conn net.Conn, b64Data string, kv *kvstore.KV) {
	data, err := base64.StdEncoding.DecodeString(b64Data)
	if err != nil {
		conn.Write([]byte("ERR watch base64 decode failed\n"))
		return
	}

	var req kvpb.WatchRequest
	if err := proto.Unmarshal(data, &req); err != nil {
		conn.Write([]byte("ERR watch proto unmarshal failed\n"))
		return
	}

	var watcher *watch.Watcher
	var err2 error

	// 根据请求类型创建监听器
	if req.Key != "" {
		if req.WatcherId != "" {
			watcher, err2 = kv.WatchKeyWithID(req.WatcherId, req.Key)
		} else {
			watcher, err2 = kv.WatchKey(req.Key)
		}
	} else if req.Prefix != "" {
		if req.WatcherId != "" {
			watcher, err2 = kv.WatchPrefixWithID(req.WatcherId, req.Prefix)
		} else {
			watcher, err2 = kv.WatchPrefix(req.Prefix)
		}
	} else {
		conn.Write([]byte("ERR either key or prefix must be specified\n"))
		return
	}

	if err2 != nil {
		conn.Write([]byte("ERR watch creation failed: " + err2.Error() + "\n"))
		return
	}

	// 创建响应
	resp := &kvpb.WatchResponse{
		WatcherId: watcher.ID,
		Success:   true,
	}

	respData, err := proto.Marshal(resp)
	if err != nil {
		conn.Write([]byte("ERR watch response marshal failed\n"))
		return
	}

	b64 := base64.StdEncoding.EncodeToString(respData)
	conn.Write([]byte(b64 + "\n"))

	// 启动事件流处理
	go handleWatchStream(conn, watcher)
}

// handleWatchStream 处理Watch事件流
func handleWatchStream(conn net.Conn, watcher *watch.Watcher) {
	defer watcher.Close()

	// 创建事件流
	stream := watch.NewWatchStream(watcher)
	defer stream.Close()

	for {
		select {
		case event := <-stream.Events():
			// 将事件转换为protobuf格式
			pbEvent := &kvpb.WatchEvent{
				Type:      int32(event.Type),
				Key:       event.Key,
				Value:     event.Value,
				Revision:  event.Revision,
				Timestamp: event.Timestamp.Unix(),
				Ttl:       event.TTL,
			}

			eventData, err := proto.Marshal(pbEvent)
			if err != nil {
				continue
			}

			b64 := base64.StdEncoding.EncodeToString(eventData)
			conn.Write([]byte("EVENT " + b64 + "\n"))

		case err := <-stream.Errors():
			conn.Write([]byte("ERROR " + err.Error() + "\n"))
			return

		case <-stream.Done():
			return
		}
	}
}

// handleUnwatchRequest 处理Unwatch请求
func handleUnwatchRequest(conn net.Conn, watcherID string, kv *kvstore.KV) {
	err := kv.Unwatch(watcherID)
	if err != nil {
		conn.Write([]byte("ERR unwatch failed: " + err.Error() + "\n"))
		return
	}

	conn.Write([]byte("OK\n"))
}

// handleWatchStatsRequest 处理Watch统计信息请求
func handleWatchStatsRequest(conn net.Conn, kv *kvstore.KV) {
	stats := kv.GetWatchStats()

	// 转换为JSON格式
	statsJSON, err := json.Marshal(stats)
	if err != nil {
		conn.Write([]byte("ERR stats marshal failed\n"))
		return
	}

	conn.Write([]byte(string(statsJSON) + "\n"))
}
