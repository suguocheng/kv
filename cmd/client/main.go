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

	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print(">> ")
		input, _ := reader.ReadString('\n')
		cmd := strings.TrimSpace(input)
		if cmd == "exit" || cmd == "quit" {
			break
		}
		resp, err := cli.SendCommand(cmd)
		if err != nil {
			fmt.Println("Error:", err)
		} else {
			fmt.Println(resp)
		}
	}
}
