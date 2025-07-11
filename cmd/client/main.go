package main

import (
	"fmt"
	"io"
	"kv/pkg/client"
	"os"
	"path/filepath"
	"strings"

	"github.com/peterh/liner"
)

func main() {
	// 所有服务器节点的地址
	servers := []string{
		"127.0.0.1:9000",
		"127.0.0.1:9001",
		"127.0.0.1:9002",
	}
	cli := client.NewClient(servers)

	printHelp()

	// 初始化 liner
	line := liner.NewLiner()
	defer line.Close()
	line.SetCtrlCAborts(true)

	// 历史命令文件存放在项目根目录/history/kvcli_history
	cwd, _ := os.Getwd()
	historyDirPath := filepath.Join(cwd, "history")
	_ = os.MkdirAll(historyDirPath, 0755)
	historyPath := filepath.Join(historyDirPath, "kvcli_history")

	// 加载历史命令
	if f, err := os.Open(historyPath); err == nil {
		line.ReadHistory(f)
		f.Close()
	}

	for {
		// 读取命令
		cmd, err := line.Prompt(">> ")
		if err != nil {
			if err == liner.ErrPromptAborted || err == io.EOF {
				break
			}
			fmt.Println("读取命令出错:", err)
			continue
		}
		cmd = strings.TrimSpace(cmd)
		if cmd == "exit" || cmd == "quit" || cmd == "EXIT" || cmd == "QUIT" {
			break
		}
		if cmd == "" {
			continue
		}
		line.AppendHistory(cmd)

		// 分割命令
		parts := strings.Fields(cmd)
		if len(parts) == 0 {
			continue
		}

		switch strings.ToUpper(parts[0]) {
		case "GETREV":
			handleGetRevision(cli, parts[1:])
		case "HISTORY":
			handleGetHistory(cli, parts[1:])
		case "RANGE":
			handleRange(cli, parts[1:])
		case "COMPACT":
			handleCompact(cli, parts[1:])
		case "STATS":
			handleStats(cli)
		case "TXN":
			handleTxn(cli, parts[1:])
		case "WATCH":
			handleWatch(cli, parts[1:])
		case "UNWATCH":
			handleUnwatch(cli, parts[1:])
		case "WATCHSTATS":
			handleWatchStats(cli)
		case "HELP":
			printHelp()
		default:
			resp, err := cli.SendCommand(cmd)
			if err != nil {
				fmt.Println("Error:", err)
			} else {
				fmt.Println(resp)
			}
		}
	}

	// 保存历史命令
	if f, err := os.Create(historyPath); err == nil {
		line.WriteHistory(f)
		f.Close()
	}
}
