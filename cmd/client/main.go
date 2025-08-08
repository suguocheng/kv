package main

import (
	"fmt"
	"io"
	"kv/pkg/client"
	"kv/pkg/config"
	"os"
	"path/filepath"
	"strings"

	"github.com/peterh/liner"
)

func main() {
	// 加载配置文件
	cfg, err := config.LoadConfig("config.env")
	if err != nil {
		fmt.Printf("Failed to load config: %v\n", err)
		panic(fmt.Sprintf("Failed to load config: %v", err))
	}

	cli := client.NewClient(cfg.Client.Servers)

	// 初始化 liner
	line := liner.NewLiner()
	defer line.Close()
	line.SetCtrlCAborts(true)

	// 历史命令文件存放在项目根目录/history/kvcli_history
	cwd, _ := os.Getwd()
	historyDirPath := filepath.Join(cwd, cfg.Client.HistoryDir)
	_ = os.MkdirAll(historyDirPath, 0755)
	historyPath := filepath.Join(historyDirPath, cfg.Client.HistoryFile)

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
		case "GET":
			handleGet(cli, parts[1:])
		case "PUT":
			handlePut(cli, parts[1:])
		case "DEL":
			handleDel(cli, parts[1:])
		case "PUTTTL":
			handlePutTTL(cli, parts[1:])
		case "TXN":
			handleTxn(cli, parts[1:])
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
		case "WATCH":
			handleWatch(cli, parts[1:])
		case "UNWATCH":
			handleUnwatch(cli, parts[1:])
		case "WATCHLIST":
			handleWatchList(cli)
		case "HELP":
			printHelp()
		default:
			fmt.Printf("Unknown command: %s\n", parts[0])
			fmt.Println("Type HELP for available commands")
		}
	}

	// 保存历史命令
	if f, err := os.Create(historyPath); err == nil {
		line.WriteHistory(f)
		f.Close()
	}
}
