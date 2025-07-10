package main

import (
	"fmt"
	"io"
	"kv/pkg/client"
	"kv/pkg/kvpb"
	"os"
	"path/filepath"
	"strconv"
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
			handleTxn(cli, line)
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

// printHelp 打印帮助信息
func printHelp() {
	fmt.Println("分布式KV存储客户端")
	fmt.Println("支持的命令:")
	fmt.Println("  基本操作:")
	fmt.Println("    GET key                       - 获取值")
	fmt.Println("    PUT key value                 - 设置值（永不过期）")
	fmt.Println("    PUTTTL key value ttl          - 设置值（指定TTL秒数）")
	fmt.Println("    DEL key                       - 删除键")
	fmt.Println("  事务操作:")
	fmt.Println("    TXN                           - 事务操作（条件执行+原子提交，或批量原子操作）")
	fmt.Println("  MVCC操作:")
	fmt.Println("    GETREV key revision           - 获取指定版本的值")
	fmt.Println("    HISTORY key [limit]           - 获取键的版本历史")
	fmt.Println("    RANGE start end [rev] [limit] - 范围查询（支持版本）")
	fmt.Println("    COMPACT revision              - 压缩版本历史")
	fmt.Println("    STATS                         - 获取统计信息")
	fmt.Println("  其他:")
	fmt.Println("    exit/quit                     - 退出")
	fmt.Println()
}

// handleGetRevision 处理版本查询
func handleGetRevision(cli *client.Client, args []string) {
	if len(args) != 2 {
		fmt.Println("用法: GETREV key revision")
		return
	}

	key := args[0]
	revision, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		fmt.Println("版本号必须是数字")
		return
	}

	value, rev, err := cli.GetWithRevision(key, revision)
	if err != nil {
		fmt.Printf("查询版本失败: %v\n", err)
		return
	}

	fmt.Printf("键: %s, 版本: %d, 值: %s\n", key, rev, value)
}

// handleGetHistory 处理历史查询
func handleGetHistory(cli *client.Client, args []string) {
	if len(args) < 1 || len(args) > 2 {
		fmt.Println("用法: HISTORY key [limit]")
		return
	}

	key := args[0]
	limit := int64(10) // 默认显示10个版本

	if len(args) == 2 {
		var err error
		limit, err = strconv.ParseInt(args[1], 10, 64)
		if err != nil {
			fmt.Println("限制数量必须是数字")
			return
		}
	}

	history, err := cli.GetHistory(key, limit)
	if err != nil {
		fmt.Printf("获取历史失败: %v\n", err)
		return
	}

	fmt.Printf("键 '%s' 的版本历史 (共%d个版本):\n", key, len(history))
	for i, version := range history {
		status := "正常"
		if version.Deleted {
			status = "已删除"
		}
		fmt.Printf("  版本%d: 值='%s', 修改版本=%d, 创建版本=%d, 状态=%s\n",
			i+1, version.Value, version.ModRev, version.CreatedRev, status)
	}
}

// handleRange 处理范围查询
func handleRange(cli *client.Client, args []string) {
	if len(args) < 2 || len(args) > 4 {
		fmt.Println("用法: RANGE start end [revision] [limit]")
		return
	}

	start := args[0]
	end := args[1]
	revision := int64(0)
	limit := int64(100)

	if len(args) >= 3 {
		var err error
		revision, err = strconv.ParseInt(args[2], 10, 64)
		if err != nil {
			fmt.Println("版本号必须是数字")
			return
		}
	}

	if len(args) >= 4 {
		var err error
		limit, err = strconv.ParseInt(args[3], 10, 64)
		if err != nil {
			fmt.Println("限制数量必须是数字")
			return
		}
	}

	results, _, err := cli.Range(start, end, revision, limit)
	if err != nil {
		fmt.Printf("范围查询失败: %v\n", err)
		return
	}

	if revision > 0 {
		fmt.Printf("范围 [%s, %s) 在版本 %d 的查询结果 (共%d个):\n", start, end, revision, len(results))
	} else {
		fmt.Printf("范围 [%s, %s) 的查询结果 (共%d个):\n", start, end, len(results))
	}

	for _, result := range results {
		status := "正常"
		if result.Deleted {
			status = "已删除"
		}
		fmt.Printf("  %s = %s (版本=%d, 状态=%s)\n", result.Key, result.Value, result.ModRev, status)
	}
}

// handleCompact 处理压缩操作
func handleCompact(cli *client.Client, args []string) {
	if len(args) != 1 {
		fmt.Println("用法: COMPACT revision")
		return
	}

	revision, err := strconv.ParseInt(args[0], 10, 64)
	if err != nil {
		fmt.Println("版本号必须是数字")
		return
	}

	compactedRev, err := cli.Compact(revision)
	if err != nil {
		fmt.Printf("压缩失败: %v\n", err)
		return
	}

	fmt.Printf("压缩完成，实际压缩到版本: %d\n", compactedRev)
}

// handleStats 处理统计信息
func handleStats(cli *client.Client) {
	stats, err := cli.GetStats()
	if err != nil {
		fmt.Printf("获取统计信息失败: %v\n", err)
		return
	}

	fmt.Println("=== 统计信息 ===")
	fmt.Printf("当前版本: %v\n", stats["current_revision"])
	fmt.Printf("压缩版本: %v\n", stats["compacted_revision"])
	fmt.Printf("总键数: %v\n", stats["total_keys"])
	fmt.Printf("总版本数: %v\n", stats["total_versions"])
	if level, ok := stats["level"]; ok {
		fmt.Printf("跳表层数: %v\n", level)
	}
}

// 事务命令处理（合并自handleTxnMode）
func handleTxn(cli *client.Client, line *liner.State) {
	fmt.Println("=== 事务操作 ===")
	fmt.Println("支持两种用法：")
	fmt.Println("1. 条件事务: IF <条件> THEN <成功操作> ELSE <失败操作>")
	fmt.Println("   条件格式: EXISTS key | VALUE key = value | VALUE key != value")
	fmt.Println("   操作格式: PUT key value | DEL key | GET key")
	fmt.Println()
	fmt.Println("2. 无条件批量原子操作: PUT foo 1 PUT bar 2 DEL baz")
	fmt.Println("输入 'back' 返回主菜单")
	for {
		cmd, err := line.Prompt("TXN>> ")
		if err != nil {
			if err == liner.ErrPromptAborted || err == io.EOF {
				break
			}
			fmt.Println("读取命令出错:", err)
			continue
		}
		cmd = strings.TrimSpace(cmd)
		if cmd == "back" || cmd == "BACK" {
			break
		}
		if cmd == "" {
			continue
		}
		line.AppendHistory(cmd)
		if !strings.Contains(cmd, "IF") && !strings.Contains(cmd, "THEN") && !strings.Contains(cmd, "ELSE") {
			ops, err := parseOps(strings.Fields(cmd))
			if err != nil {
				fmt.Println("批量操作解析错误:", err)
				continue
			}
			txnReq := &kvpb.TxnRequest{
				Compare: nil,
				Success: ops,
				Failure: nil,
			}
			resp, err := cli.Txn(txnReq)
			if err != nil {
				fmt.Println("批量操作执行错误:", err)
			} else {
				fmt.Printf("批量操作结果: %s\n", formatTxnResponse(resp))
			}
			continue
		}
		txnReq, err := parseTxnCommand(cmd)
		if err != nil {
			fmt.Println("事务解析错误:", err)
			continue
		}
		resp, err := cli.Txn(txnReq)
		if err != nil {
			fmt.Println("事务执行错误:", err)
		} else {
			fmt.Printf("事务结果: %s\n", formatTxnResponse(resp))
		}
	}
}

// parseTxnCommand 解析事务命令
func parseTxnCommand(cmd string) (*kvpb.TxnRequest, error) {
	parts := strings.Fields(cmd)
	if len(parts) < 3 || parts[0] != "IF" {
		return nil, fmt.Errorf("事务格式错误，应以 'IF' 开头")
	}

	// 查找 THEN 和 ELSE
	thenIdx := -1
	elseIdx := -1
	for i, part := range parts {
		if part == "THEN" {
			thenIdx = i
		} else if part == "ELSE" {
			elseIdx = i
		}
	}

	if thenIdx == -1 {
		return nil, fmt.Errorf("缺少 THEN 子句")
	}

	// 解析条件
	compare, err := parseCompare(parts[1:thenIdx])
	if err != nil {
		return nil, err
	}

	// 解析成功操作
	var successOps []*kvpb.Op
	if elseIdx != -1 {
		successOps, err = parseOps(parts[thenIdx+1 : elseIdx])
	} else {
		successOps, err = parseOps(parts[thenIdx+1:])
	}
	if err != nil {
		return nil, err
	}

	// 解析失败操作
	var failureOps []*kvpb.Op
	if elseIdx != -1 {
		failureOps, err = parseOps(parts[elseIdx+1:])
		if err != nil {
			return nil, err
		}
	}

	return &kvpb.TxnRequest{
		Compare: []*kvpb.Compare{compare},
		Success: successOps,
		Failure: failureOps,
	}, nil
}

// parseCompare 解析比较条件
func parseCompare(parts []string) (*kvpb.Compare, error) {
	if len(parts) < 2 {
		return nil, fmt.Errorf("条件格式错误")
	}

	switch parts[0] {
	case "EXISTS":
		if len(parts) != 2 {
			return nil, fmt.Errorf("EXISTS 条件格式: EXISTS key")
		}
		return &kvpb.Compare{
			Key:    parts[1],
			Target: kvpb.CompareTarget_EXISTS,
			Result: kvpb.CompareResult_EQUAL,
		}, nil
	case "VALUE":
		if len(parts) != 4 {
			return nil, fmt.Errorf("VALUE 条件格式: VALUE key = value 或 VALUE key != value")
		}
		var result kvpb.CompareResult
		switch parts[2] {
		case "=":
			result = kvpb.CompareResult_EQUAL
		case "!=":
			result = kvpb.CompareResult_NOT_EQUAL
		default:
			return nil, fmt.Errorf("不支持的比较操作: %s", parts[2])
		}
		return &kvpb.Compare{
			Key:    parts[1],
			Target: kvpb.CompareTarget_VALUE,
			Result: result,
			Value:  parts[3],
		}, nil
	default:
		return nil, fmt.Errorf("不支持的条件类型: %s", parts[0])
	}
}

// parseOps 解析操作列表
func parseOps(parts []string) ([]*kvpb.Op, error) {
	if len(parts) == 0 {
		return nil, nil
	}

	var ops []*kvpb.Op
	for i := 0; i < len(parts); {
		if i+1 >= len(parts) {
			return nil, fmt.Errorf("操作参数不完整")
		}

		opType := strings.ToUpper(parts[i])
		key := parts[i+1]

		switch opType {
		case "GET":
			ops = append(ops, &kvpb.Op{
				Type: "GET",
				Key:  key,
			})
			i += 2
		case "PUT":
			if i+2 >= len(parts) {
				return nil, fmt.Errorf("PUT 操作需要值参数")
			}
			ops = append(ops, &kvpb.Op{
				Type:  "PUT",
				Key:   key,
				Value: parts[i+2],
			})
			i += 3
		case "DEL":
			ops = append(ops, &kvpb.Op{
				Type: "DEL",
				Key:  key,
			})
			i += 2
		default:
			return nil, fmt.Errorf("不支持的操作类型: %s", opType)
		}
	}

	return ops, nil
}

// formatTxnResponse 格式化事务响应
func formatTxnResponse(resp *kvpb.TxnResponse) string {
	if resp.Succeeded {
		return fmt.Sprintf("成功 (执行了 %d 个操作)", len(resp.Responses))
	} else {
		return fmt.Sprintf("失败 (执行了 %d 个操作)", len(resp.Responses))
	}
}
