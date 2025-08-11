package client

import (
	"context"
	"fmt"
	"kv/pkg/proto/kvpb"
	"os"
	"strconv"
	"strings"
	"time"

	"sync"
)

type Client struct {
	servers []string        // 节点地址列表
	leader  int             // 上一次成功的 leader index
	pool    *ConnectionPool // 连接池
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
	stream   kvpb.KVService_WatchClient
	events   chan *WatchEvent
	errors   chan error
	done     chan struct{}
	mu       sync.RWMutex
	isClosed bool
}

// NewWatchStream 创建新的Watch流
func NewWatchStream(stream kvpb.KVService_WatchClient) *WatchStream {
	ws := &WatchStream{
		stream: stream,
		events: make(chan *WatchEvent, 100),
		errors: make(chan error, 10),
		done:   make(chan struct{}),
	}

	go ws.readEvents()
	return ws
}

// readEvents 读取事件流
func (ws *WatchStream) readEvents() {
	defer func() {
		ws.mu.Lock()
		defer ws.mu.Unlock()
		if !ws.isClosed {
			ws.isClosed = true
			close(ws.events)
			close(ws.errors)
			close(ws.done)
		}
	}()

	for {
		select {
		case <-ws.done:
			return
		default:
			watchEvent, err := ws.stream.Recv()
			if err != nil {
				ws.errors <- fmt.Errorf("read error: %v", err)
				return
			}

			// 直接解析WatchEvent
			event := &WatchEvent{
				Type:      int(watchEvent.Type),
				Key:       watchEvent.Key,
				Value:     watchEvent.Value,
				Revision:  watchEvent.Revision,
				Timestamp: watchEvent.Timestamp,
				TTL:       watchEvent.Ttl,
			}

			select {
			case ws.events <- event:
			case <-ws.done:
				return
			}
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
	ws.mu.Lock()
	defer ws.mu.Unlock()
	if !ws.isClosed {
		ws.isClosed = true
		close(ws.done)
		ws.stream.CloseSend()
	}
}

func NewClient(servers []string) *Client {
	// 检测是否在测试环境中
	var poolConfig *PoolConfig
	if isTestEnvironment() {
		poolConfig = TestPoolConfig()
	}
	return NewClientWithPool(servers, poolConfig)
}

// isTestEnvironment 检测是否在测试环境中
func isTestEnvironment() bool {
	// 检查是否在go test环境中
	return strings.Contains(os.Args[0], "test") ||
		strings.Contains(os.Args[0], ".test") ||
		len(os.Args) > 0 && strings.Contains(os.Args[0], "go-build")
}

// NewClientWithPool 创建带连接池的客户端
func NewClientWithPool(servers []string, poolConfig *PoolConfig) *Client {
	// 创建连接池
	pool := NewConnectionPool(poolConfig)

	// 初始化连接池中的服务器
	for _, addr := range servers {
		if err := pool.AddServer(addr); err != nil {
			fmt.Printf("Warning: failed to add server %s to pool: %v\n", addr, err)
		}
	}

	return &Client{
		servers: servers,
		leader:  0, // 默认先尝试第一个
		pool:    pool,
	}
}

// getClientFromPool 从连接池获取客户端
func (c *Client) getClientFromPool(serverIndex int) (kvpb.KVServiceClient, error) {
	if c.pool != nil && serverIndex < len(c.servers) {
		return c.pool.GetClient(c.servers[serverIndex])
	}
	return nil, fmt.Errorf("no client available for server index %d", serverIndex)
}

// Put 写入键值对
func (c *Client) Put(ctx context.Context, key, value string) (string, error) {
	req := &kvpb.PutRequest{Key: key, Value: value}

	// 尝试所有服务器
	for i := 0; i < len(c.servers); i++ {
		idx := (c.leader + i) % len(c.servers)

		client, err := c.getClientFromPool(idx)
		if err != nil {
			continue
		}

		resp, err := client.Put(ctx, req)
		if err != nil {
			continue
		}

		if resp.Error == "" {
			c.leader = idx
			return "OK", nil
		} else if strings.Contains(resp.Error, "Not_Leader") {
			continue
		} else {
			return "", fmt.Errorf(resp.Error)
		}
	}

	return "", fmt.Errorf("no available leader")
}

// PutWithTTL 写入带TTL的键值对
func (c *Client) PutWithTTL(ctx context.Context, key, value, ttlStr string) (string, error) {
	// 解析TTL
	ttl, err := strconv.ParseInt(ttlStr, 10, 64)
	if err != nil {
		return "", fmt.Errorf("invalid TTL: %v", err)
	}

	req := &kvpb.PutWithTTLRequest{Key: key, Value: value, Ttl: ttl}

	// 尝试所有服务器
	for i := 0; i < len(c.servers); i++ {
		idx := (c.leader + i) % len(c.servers)

		client, err := c.getClientFromPool(idx)
		if err != nil {
			continue
		}

		resp, err := client.PutWithTTL(ctx, req)
		if err != nil {
			continue
		}

		if resp.Error == "" {
			c.leader = idx
			return "OK", nil
		} else if strings.Contains(resp.Error, "Not_Leader") {
			continue
		} else {
			return "", fmt.Errorf(resp.Error)
		}
	}

	return "", fmt.Errorf("no available leader")
}

// Get 获取键值对
func (c *Client) Get(ctx context.Context, key string) (string, error) {
	req := &kvpb.GetRequest{Key: key}

	// 尝试所有服务器
	for i := 0; i < len(c.servers); i++ {
		idx := (c.leader + i) % len(c.servers)

		client, err := c.getClientFromPool(idx)
		if err != nil {
			continue
		}

		resp, err := client.Get(ctx, req)
		if err != nil {
			continue
		}

		if resp.Exists {
			c.leader = idx
			return resp.Value, nil
		} else {
			return "NOTFOUND", nil
		}
	}

	return "", fmt.Errorf("no available server")
}

// GetWithRevision 获取指定版本的键值对
func (c *Client) GetWithRevision(key string, revision int64) (string, int64, error) {
	ctx := context.Background()
	req := &kvpb.GetWithRevisionRequest{Key: key, Revision: revision}

	// 尝试所有服务器
	for i := 0; i < len(c.servers); i++ {
		idx := (c.leader + i) % len(c.servers)

		client, err := c.getClientFromPool(idx)
		if err != nil {
			continue
		}

		resp, err := client.GetWithRevision(ctx, req)
		if err != nil {
			continue
		}

		if resp.Exists {
			c.leader = idx
			return resp.Value, resp.Revision, nil
		} else {
			return "", 0, fmt.Errorf("key not found")
		}
	}

	return "", 0, fmt.Errorf("no available server")
}

// Delete 删除键
func (c *Client) Delete(ctx context.Context, key string) (string, error) {
	req := &kvpb.DeleteRequest{Key: key}

	// 尝试所有服务器
	for i := 0; i < len(c.servers); i++ {
		idx := (c.leader + i) % len(c.servers)

		client, err := c.getClientFromPool(idx)
		if err != nil {
			continue
		}

		resp, err := client.Delete(ctx, req)
		if err != nil {
			continue
		}

		if resp.Error == "" {
			c.leader = idx
			return "OK", nil
		} else if strings.Contains(resp.Error, "Not_Leader") {
			continue
		} else {
			return "", fmt.Errorf(resp.Error)
		}
	}

	return "", fmt.Errorf("no available leader")
}

// GetHistory 获取键的版本历史
func (c *Client) GetHistory(key string, limit int64) ([]*VersionedKV, error) {
	ctx := context.Background()
	req := &kvpb.GetHistoryRequest{Key: key, Limit: limit}

	// 尝试所有服务器
	for i := 0; i < len(c.servers); i++ {
		idx := (c.leader + i) % len(c.servers)

		client, err := c.getClientFromPool(idx)
		if err != nil {
			continue
		}

		resp, err := client.GetHistory(ctx, req)
		if err != nil {
			continue
		}

		if resp.Error != "" {
			return nil, fmt.Errorf(resp.Error)
		}

		// 转换为VersionedKV
		var versions []*VersionedKV
		for _, pair := range resp.Items {
			versions = append(versions, &VersionedKV{
				Key:        pair.Key,
				Value:      pair.Value,
				TTL:        pair.Ttl,
				CreatedRev: pair.CreatedRevision,
				ModRev:     pair.ModRevision,
				Version:    pair.Version,
				Deleted:    pair.Deleted,
				CreatedAt:  time.Now().Unix(),
			})
		}

		c.leader = idx
		return versions, nil
	}

	return nil, fmt.Errorf("no available server")
}

// Range 范围查询
func (c *Client) Range(start, end string, revision int64, limit int64) ([]*VersionedKV, int64, error) {
	ctx := context.Background()
	req := &kvpb.RangeRequest{
		Key:      start,
		RangeEnd: end,
		Revision: revision,
		Limit:    limit,
	}

	// 尝试所有服务器
	for i := 0; i < len(c.servers); i++ {
		idx := (c.leader + i) % len(c.servers)

		client, err := c.getClientFromPool(idx)
		if err != nil {
			continue
		}

		resp, err := client.Range(ctx, req)
		if err != nil {
			continue
		}

		// RangeResponse没有Error字段，直接处理结果

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
				CreatedAt:  time.Now().Unix(),
			})
		}

		c.leader = idx
		return results, resp.Revision, nil
	}

	return nil, 0, fmt.Errorf("no available server")
}

// Compact 压缩版本历史
func (c *Client) Compact(revision int64) (int64, error) {
	ctx := context.Background()
	req := &kvpb.CompactRequest{
		Revision: revision,
		Physical: true,
	}

	// 尝试所有服务器
	for i := 0; i < len(c.servers); i++ {
		idx := (c.leader + i) % len(c.servers)

		client, err := c.getClientFromPool(idx)
		if err != nil {
			continue
		}

		resp, err := client.Compact(ctx, req)
		if err != nil {
			continue
		}

		c.leader = idx
		return resp.Revision, nil
	}

	return 0, fmt.Errorf("no available server")
}

// GetStats 获取统计信息
func (c *Client) GetStats() (map[string]interface{}, error) {
	ctx := context.Background()
	req := &kvpb.GetStatsRequest{}

	// 尝试所有服务器
	for i := 0; i < len(c.servers); i++ {
		idx := (c.leader + i) % len(c.servers)

		client, err := c.getClientFromPool(idx)
		if err != nil {
			continue
		}

		resp, err := client.GetStats(ctx, req)
		if err != nil {
			continue
		}

		if resp.Error != "" {
			return nil, fmt.Errorf(resp.Error)
		}

		// 转换为map[string]interface{}
		stats := make(map[string]interface{})
		for key, value := range resp.Stats {
			stats[key] = value
		}

		c.leader = idx
		return stats, nil
	}

	return nil, fmt.Errorf("no available server")
}

// Txn 发送事务请求，返回TxnResponse
func (c *Client) Txn(req *kvpb.TxnRequest) (*kvpb.TxnResponse, error) {
	ctx := context.Background()

	// 尝试所有服务器
	for i := 0; i < len(c.servers); i++ {
		idx := (c.leader + i) % len(c.servers)

		client, err := c.getClientFromPool(idx)
		if err != nil {
			continue
		}

		resp, err := client.Txn(ctx, req)
		if err != nil {
			continue
		}

		c.leader = idx
		return resp, nil
	}

	return nil, fmt.Errorf("no available server")
}

// Watch相关方法

// WatchKeyWithID 使用指定ID监听键的变化
func (c *Client) WatchKeyWithID(key, watcherID string) (*WatchStream, error) {
	req := &kvpb.WatchRequest{
		Key:       key,
		WatcherId: watcherID,
	}

	return c.sendWatchRequest(req)
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
	ctx := context.Background()

	// 尝试所有服务器
	for i := 0; i < len(c.servers); i++ {
		idx := (c.leader + i) % len(c.servers)

		client, err := c.getClientFromPool(idx)
		if err != nil {
			continue
		}

		stream, err := client.Watch(ctx, req)
		if err != nil {
			continue
		}

		// 创建Watch流
		return NewWatchStream(stream), nil
	}

	return nil, fmt.Errorf("no server available")
}

// Unwatch 取消监听
func (c *Client) Unwatch(watcherID string) error {
	ctx := context.Background()
	req := &kvpb.UnwatchRequest{WatcherId: watcherID}
	for i := 0; i < len(c.servers); i++ {
		idx := (c.leader + i) % len(c.servers)

		client, err := c.getClientFromPool(idx)
		if err != nil {
			continue
		}

		resp, err := client.Unwatch(ctx, req)
		if err == nil && resp.Success {
			c.leader = idx
			return nil // 成功
		}
	}
	return fmt.Errorf("取消监听失败: watcher with ID %s not found", watcherID)
}

// GetWatchList 获取活跃监听器列表
func (c *Client) GetWatchList() (*kvpb.GetWatchListResponse, error) {
	ctx := context.Background()
	req := &kvpb.GetWatchListRequest{}

	// 尝试所有服务器
	for i := 0; i < len(c.servers); i++ {
		idx := (c.leader + i) % len(c.servers)

		client, err := c.getClientFromPool(idx)
		if err != nil {
			continue
		}

		resp, err := client.GetWatchList(ctx, req)
		if err != nil {
			continue
		}

		c.leader = idx
		return resp, nil
	}

	return nil, fmt.Errorf("no available server")
}

// Close 关闭客户端连接池
func (c *Client) Close() {
	if c.pool != nil {
		c.pool.Close()
	}
}

// GetPoolStats 获取连接池统计信息
func (c *Client) GetPoolStats() map[string]interface{} {
	if c.pool != nil {
		return c.pool.GetStats()
	}
	return nil
}
