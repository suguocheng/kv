package client

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"time"
)

type Client struct {
	servers []string // 节点地址列表
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
	cmdType := strings.ToUpper(strings.Fields(cmd)[0])
	isRead := (cmdType == "GET")

	// 对于 GET 请求，任意一个节点即可
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
		return "", fmt.Errorf("no node available for GET")
	}

	// 对于 PUT / DEL 请求，必须找 Leader
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
