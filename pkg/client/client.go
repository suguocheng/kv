package client

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"time"
)

type Client struct {
	servers []string // 节点地址列表，如 ["127.0.0.1:9000", "127.0.0.1:9001", ...]
	leader  int      // 上一次成功的 leader index
}

func NewClient(servers []string) *Client {
	return &Client{
		servers: servers,
		leader:  0, // 默认先尝试第一个
	}
}

// 向 Leader 发送命令（自动发现 Leader）
func (c *Client) SendCommand(cmd string) (string, error) {
	// 优先尝试上次成功的 Leader
	for i := 0; i < len(c.servers); i++ {
		idx := (c.leader + i) % len(c.servers)
		addr := c.servers[idx]

		conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
		if err != nil {
			continue
		}
		defer conn.Close()

		// 发送命令
		fmt.Fprintln(conn, cmd)

		// 读取返回
		reply, err := bufio.NewReader(conn).ReadString('\n')
		if err != nil {
			continue
		}
		reply = strings.TrimSpace(reply)

		// 判断是否是 Leader 响应
		if reply == "NOT_LEADER" {
			continue
		}

		// 成功响应
		c.leader = idx // 更新缓存
		return reply, nil
	}

	return "", fmt.Errorf("no available leader found")
}
