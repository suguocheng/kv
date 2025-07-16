package client

import (
	"bufio"
	"encoding/base64"
	"encoding/json" // Added for json.Unmarshal
	"fmt"
	"kv/pkg/proto/kvpb"
	"net"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"
)

type Client struct {
	servers []string // 节点地址列表
	leader  int      // 上一次成功的 leader index
}

// VersionedKV 客户端版本信息结构
type VersionedKV struct {
	Key        string
	Value      string
	TTL        int64
	CreatedRev int64
	ModRev     int64
	Version    int64
	Deleted    bool
	CreatedAt  int64
}

// WatchEvent 客户端Watch事件结构
type WatchEvent struct {
	Type      int    `json:"type"`
	Key       string `json:"key"`
	Value     string `json:"value"`
	Revision  int64  `json:"revision"`
	Timestamp int64  `json:"timestamp"`
	TTL       int64  `json:"ttl"`
}

// String 返回事件的字符串表示
func (e *WatchEvent) String() string {
	eventType := "UNKNOWN"
	switch e.Type {
	case 0:
		eventType = "PUT"
	case 1:
		eventType = "DELETE"
	case 2:
		eventType = "EXPIRE"
	}

	return fmt.Sprintf("Event{Type: %s, Key: %s, Value: %s, Revision: %d, Timestamp: %d, TTL: %d}",
		eventType, e.Key, e.Value, e.Revision, e.Timestamp, e.TTL)
}

// WatchStream 客户端Watch流
type WatchStream struct {
	conn   net.Conn
	events chan *WatchEvent
	errors chan error
	done   chan struct{}
}

// NewWatchStream 创建新的Watch流
func NewWatchStream(conn net.Conn) *WatchStream {
	stream := &WatchStream{
		conn:   conn,
		events: make(chan *WatchEvent, 100),
		errors: make(chan error, 10),
		done:   make(chan struct{}),
	}

	go stream.readEvents()
	return stream
}

// readEvents 读取事件流
func (ws *WatchStream) readEvents() {
	defer close(ws.events)
	defer close(ws.errors)
	defer close(ws.done)

	reader := bufio.NewReader(ws.conn)

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			ws.errors <- fmt.Errorf("read error: %v", err)
			return
		}

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		parts := strings.Fields(line)
		if len(parts) == 0 {
			continue
		}

		switch parts[0] {
		case "EVENT":
			if len(parts) != 2 {
				continue
			}

			// 解析事件
			eventData, err := base64.StdEncoding.DecodeString(parts[1])
			if err != nil {
				continue
			}

			var pbEvent kvpb.WatchEvent
			if err := proto.Unmarshal(eventData, &pbEvent); err != nil {
				continue
			}

			event := &WatchEvent{
				Type:      int(pbEvent.Type),
				Key:       pbEvent.Key,
				Value:     pbEvent.Value,
				Revision:  pbEvent.Revision,
				Timestamp: pbEvent.Timestamp,
				TTL:       pbEvent.Ttl,
			}

			select {
			case ws.events <- event:
			case <-ws.done:
				return
			}

		case "ERROR":
			if len(parts) >= 2 {
				errorMsg := strings.Join(parts[1:], " ")
				ws.errors <- fmt.Errorf("server error: %s", errorMsg)
			}
			return

		default:
			// 忽略其他消息
		}
	}
}

// Events 返回事件通道
func (ws *WatchStream) Events() <-chan *WatchEvent {
	return ws.events
}

// Errors 返回错误通道
func (ws *WatchStream) Errors() <-chan error {
	return ws.errors
}

// Close 关闭Watch流
func (ws *WatchStream) Close() {
	close(ws.done)
	ws.conn.Close()
}

func NewClient(servers []string) *Client {
	return &Client{
		servers: servers,
		leader:  0, // 默认先尝试第一个
	}
}

// 向 Leader 发送命令（自动发现 Leader）
func (c *Client) SendCommand(cmd string) (string, error) {
	cmdType := strings.ToUpper(strings.Fields(cmd)[0])
	isRead := (cmdType == "GET" || cmdType == "GETREV" || cmdType == "HISTORY" || cmdType == "RANGE" || cmdType == "STATS")

	// 对于读请求，任意一个节点即可
	if isRead {
		for _, addr := range c.servers {
			conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
			if err != nil {
				continue
			}
			defer conn.Close()

			fmt.Fprintln(conn, cmd)

			reply, err := bufio.NewReader(conn).ReadString('\n')
			if err != nil {
				continue
			}
			return strings.TrimSpace(reply), nil
		}
		return "", fmt.Errorf("no node available for %s", cmdType)
	}

	// 对于写请求，必须找 Leader
	for i := 0; i < len(c.servers); i++ {
		idx := (c.leader + i) % len(c.servers)
		addr := c.servers[idx]

		conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
		if err != nil {
			continue
		}
		defer conn.Close()

		fmt.Fprintln(conn, cmd)

		reply, err := bufio.NewReader(conn).ReadString('\n')
		if err != nil {
			continue
		}
		reply = strings.TrimSpace(reply)

		if reply == "Not_Leader" {
			continue
		}

		c.leader = idx
		return reply, nil
	}

	return "", fmt.Errorf("no available leader found for %s", cmdType)
}

// Put 写入键值对
func (c *Client) Put(key, value string) error {
	cmd := fmt.Sprintf("PUT %s %s", key, value)
	_, err := c.SendCommand(cmd)
	return err
}

// PutWithTTL 写入带TTL的键值对
func (c *Client) PutWithTTL(key, value string, ttl int64) error {
	cmd := fmt.Sprintf("PUTTTL %s %s %d", key, value, ttl)
	_, err := c.SendCommand(cmd)
	return err
}

// Get 获取键值对
func (c *Client) Get(key string) (string, error) {
	cmd := fmt.Sprintf("GET %s", key)
	return c.SendCommand(cmd)
}

// GetWithRevision 获取指定版本的键值对
func (c *Client) GetWithRevision(key string, revision int64) (string, int64, error) {
	cmd := fmt.Sprintf("GETREV %s %d", key, revision)
	resp, err := c.SendCommand(cmd)
	if err != nil {
		return "", 0, err
	}

	parts := strings.SplitN(resp, " ", 2)
	if len(parts) != 2 {
		return "", 0, fmt.Errorf("invalid response format")
	}

	rev, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return "", 0, err
	}

	return parts[1], rev, nil
}

// Delete 删除键
func (c *Client) Delete(key string) error {
	cmd := fmt.Sprintf("DELETE %s", key)
	_, err := c.SendCommand(cmd)
	return err
}

// GetHistory 获取键的版本历史
func (c *Client) GetHistory(key string, limit int64) ([]*VersionedKV, error) {
	req := &kvpb.HistoryRequest{
		Key:   key,
		Limit: limit,
	}

	data, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}

	b64 := base64.StdEncoding.EncodeToString(data)
	cmd := "HISTORY " + b64

	respStr, err := c.SendCommand(cmd)
	if err != nil {
		return nil, err
	}

	respData, err := base64.StdEncoding.DecodeString(respStr)
	if err != nil {
		return nil, err
	}

	var resp kvpb.HistoryResponse
	if err := proto.Unmarshal(respData, &resp); err != nil {
		return nil, err
	}

	// 转换为VersionedKV
	var versions []*VersionedKV
	for _, pair := range resp.History {
		versions = append(versions, &VersionedKV{
			Key:        pair.Key,
			Value:      pair.Value,
			TTL:        pair.Ttl,
			CreatedRev: pair.CreatedRevision,
			ModRev:     pair.ModRevision,
			Version:    pair.Version,
			Deleted:    pair.Deleted,
			CreatedAt:  time.Now().Unix(), // 服务器时间戳
		})
	}

	return versions, nil
}

// Range 范围查询
func (c *Client) Range(start, end string, revision int64, limit int64) ([]*VersionedKV, int64, error) {
	req := &kvpb.RangeRequest{
		Key:      start,
		RangeEnd: end,
		Revision: revision,
		Limit:    limit,
	}

	data, err := proto.Marshal(req)
	if err != nil {
		return nil, 0, err
	}

	b64 := base64.StdEncoding.EncodeToString(data)
	cmd := "RANGE " + b64

	respStr, err := c.SendCommand(cmd)
	if err != nil {
		return nil, 0, err
	}

	respData, err := base64.StdEncoding.DecodeString(respStr)
	if err != nil {
		return nil, 0, err
	}

	var resp kvpb.RangeResponse
	if err := proto.Unmarshal(respData, &resp); err != nil {
		return nil, 0, err
	}

	// 转换为VersionedKV
	var results []*VersionedKV
	for _, pair := range resp.Kvs {
		results = append(results, &VersionedKV{
			Key:        pair.Key,
			Value:      pair.Value,
			TTL:        pair.Ttl,
			CreatedRev: pair.CreatedRevision,
			ModRev:     pair.ModRevision,
			Version:    pair.Version,
			Deleted:    pair.Deleted,
			CreatedAt:  time.Now().Unix(), // 服务器时间戳
		})
	}

	return results, resp.Revision, nil
}

// Compact 压缩版本历史
func (c *Client) Compact(revision int64) (int64, error) {
	req := &kvpb.CompactRequest{
		Revision: revision,
		Physical: true,
	}

	data, err := proto.Marshal(req)
	if err != nil {
		return 0, err
	}

	b64 := base64.StdEncoding.EncodeToString(data)
	cmd := "COMPACT " + b64

	respStr, err := c.SendCommand(cmd)
	if err != nil {
		return 0, err
	}

	respData, err := base64.StdEncoding.DecodeString(respStr)
	if err != nil {
		return 0, err
	}

	var resp kvpb.CompactResponse
	if err := proto.Unmarshal(respData, &resp); err != nil {
		return 0, err
	}

	return resp.Revision, nil
}

// GetStats 获取统计信息
func (c *Client) GetStats() (map[string]interface{}, error) {
	cmd := "STATS"
	resp, err := c.SendCommand(cmd)
	if err != nil {
		return nil, err
	}

	// 简单的统计信息解析（实际应该使用protobuf）
	stats := make(map[string]interface{})
	parts := strings.Split(resp, " ")
	for i := 0; i < len(parts); i += 2 {
		if i+1 < len(parts) {
			key := parts[i]
			value := parts[i+1]
			stats[key] = value
		}
	}

	return stats, nil
}

// Txn 发送事务请求，返回TxnResponse
func (c *Client) Txn(req *kvpb.TxnRequest) (*kvpb.TxnResponse, error) {
	data, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}
	b64 := base64.StdEncoding.EncodeToString(data)
	cmd := "TXN " + b64
	respStr, err := c.SendCommand(cmd)
	if err != nil {
		return nil, err
	}
	respData, err := base64.StdEncoding.DecodeString(respStr)
	if err != nil {
		return nil, err
	}
	var resp kvpb.TxnResponse
	if err := proto.Unmarshal(respData, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// Watch相关方法

// WatchKey 监听指定键的变化
func (c *Client) WatchKey(key string) (*WatchStream, error) {
	return c.WatchKeyWithID(key, "")
}

// WatchKeyWithID 使用指定ID监听键的变化
func (c *Client) WatchKeyWithID(key, watcherID string) (*WatchStream, error) {
	req := &kvpb.WatchRequest{
		Key:       key,
		WatcherId: watcherID,
	}

	return c.sendWatchRequest(req)
}

// WatchPrefix 监听指定前缀的所有键的变化
func (c *Client) WatchPrefix(prefix string) (*WatchStream, error) {
	return c.WatchPrefixWithID(prefix, "")
}

// WatchPrefixWithID 使用指定ID监听前缀的变化
func (c *Client) WatchPrefixWithID(prefix, watcherID string) (*WatchStream, error) {
	req := &kvpb.WatchRequest{
		Prefix:    prefix,
		WatcherId: watcherID,
	}

	return c.sendWatchRequest(req)
}

// sendWatchRequest 发送Watch请求
func (c *Client) sendWatchRequest(req *kvpb.WatchRequest) (*WatchStream, error) {
	data, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}

	b64 := base64.StdEncoding.EncodeToString(data)
	cmd := "WATCH " + b64

	// 连接到任意一个可用的服务器
	var conn net.Conn
	var err2 error

	for _, addr := range c.servers {
		conn, err2 = net.DialTimeout("tcp", addr, 500*time.Millisecond)
		if err2 == nil {
			break
		}
	}

	if conn == nil {
		return nil, fmt.Errorf("no server available")
	}

	// 发送Watch命令
	fmt.Fprintln(conn, cmd)

	// 读取响应
	reader := bufio.NewReader(conn)
	response, err := reader.ReadString('\n')
	if err != nil {
		conn.Close()
		return nil, err
	}

	response = strings.TrimSpace(response)
	if strings.HasPrefix(response, "ERR") {
		conn.Close()
		return nil, fmt.Errorf("watch failed: %s", response)
	}

	// 解析响应
	respData, err := base64.StdEncoding.DecodeString(response)
	if err != nil {
		conn.Close()
		return nil, err
	}

	var resp kvpb.WatchResponse
	if err := proto.Unmarshal(respData, &resp); err != nil {
		conn.Close()
		return nil, err
	}

	if !resp.Success {
		conn.Close()
		return nil, fmt.Errorf("watch failed: %s", resp.Error)
	}

	// 创建Watch流
	return NewWatchStream(conn), nil
}

// Unwatch 取消监听
func (c *Client) Unwatch(watcherID string) error {
	cmd := fmt.Sprintf("UNWATCH %s", watcherID)
	_, err := c.SendCommand(cmd)
	return err
}

// GetWatchStats 获取Watch统计信息
func (c *Client) GetWatchStats() (map[string]interface{}, error) {
	cmd := "WATCHSTATS"
	resp, err := c.SendCommand(cmd)
	if err != nil {
		return nil, err
	}

	// 解析JSON响应
	var stats map[string]interface{}
	if err := json.Unmarshal([]byte(resp), &stats); err != nil {
		return nil, err
	}

	return stats, nil
}
