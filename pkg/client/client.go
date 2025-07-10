package client

import (
	"bufio"
	"encoding/base64"
	"fmt"
	"kv/pkg/kvpb"
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
