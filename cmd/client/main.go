package main

import (
	"bufio"
	"fmt"
	"kv/pkg/client"
	"os"
	"strings"
)

func main() {
	// 所有服务器节点的地址
	servers := []string{
		"127.0.0.1:9000",
		"127.0.0.1:9001",
		"127.0.0.1:9002",
	}
	cli := client.NewClient(servers)

	fmt.Println("分布式KV存储客户端")
	fmt.Println("支持的命令:")
	fmt.Println("  GET key                    - 获取值")
	fmt.Println("  PUT key value              - 设置值（永不过期）")
	fmt.Println("  PUTTTL key value ttl       - 设置值（指定TTL秒数）")
	fmt.Println("  DEL key                    - 删除键")
	fmt.Println("  exit/quit                  - 退出")
	fmt.Println("示例:")
	fmt.Println("  PUT config_key config_value")
	fmt.Println("  PUTTTL cache_key cache_value 300")
	fmt.Println("  PUTTTL session_key session_data 7200")
	fmt.Println()

	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print(">> ")
		input, _ := reader.ReadString('\n')
		cmd := strings.TrimSpace(input)
		if cmd == "exit" || cmd == "quit" {
			break
		}
		if cmd == "" {
			continue
		}
		resp, err := cli.SendCommand(cmd)
		if err != nil {
			fmt.Println("Error:", err)
		} else {
			fmt.Println(resp)
		}
	}
}
