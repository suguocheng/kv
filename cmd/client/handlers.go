package main

import (
	"fmt"
	"kv/pkg/client"
	"kv/pkg/kvpb"
	"strconv"
	"strings"
)

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
				Value: []byte(parts[i+2]),
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

// handleWatch 处理Watch命令
func handleWatch(cli *client.Client, args []string) {
	if len(args) < 1 {
		fmt.Println("用法: WATCH key|prefix [watcher_id]")
		fmt.Println("  例如: WATCH user:1")
		fmt.Println("  例如: WATCH user: my-watcher-1")
		return
	}

	key := args[0]
	var watcherID string
	if len(args) >= 2 {
		watcherID = args[1]
	}

	var stream *client.WatchStream
	var err error

	// 判断是键还是前缀
	if strings.HasSuffix(key, ":") {
		// 前缀监听
		if watcherID != "" {
			stream, err = cli.WatchPrefixWithID(key, watcherID)
		} else {
			stream, err = cli.WatchPrefix(key)
		}
	} else {
		// 键监听
		if watcherID != "" {
			stream, err = cli.WatchKeyWithID(key, watcherID)
		} else {
			stream, err = cli.WatchKey(key)
		}
	}

	if err != nil {
		fmt.Printf("创建监听器失败: %v\n", err)
		return
	}

	fmt.Printf("开始监听 %s...\n", key)
	if watcherID != "" {
		fmt.Printf("监听器ID: %s\n", watcherID)
	}

	// 启动事件监听
	go func() {
		defer stream.Close()

		for {
			select {
			case event := <-stream.Events():
				fmt.Printf("事件: %s\n", event.String())
			case err := <-stream.Errors():
				fmt.Printf("监听错误: %v\n", err)
				return
			}
		}
	}()
}

// handleUnwatch 处理Unwatch命令
func handleUnwatch(cli *client.Client, args []string) {
	if len(args) != 1 {
		fmt.Println("用法: UNWATCH watcher_id")
		return
	}

	watcherID := args[0]
	err := cli.Unwatch(watcherID)
	if err != nil {
		fmt.Printf("取消监听失败: %v\n", err)
		return
	}

	fmt.Printf("成功取消监听器: %s\n", watcherID)
}

// handleWatchStats 处理Watch统计信息命令
func handleWatchStats(cli *client.Client) {
	stats, err := cli.GetWatchStats()
	if err != nil {
		fmt.Printf("获取Watch统计信息失败: %v\n", err)
		return
	}

	fmt.Println("\nWatch统计信息:")
	fmt.Println("-------------------------------")
	fmt.Printf("%-20s | %s\n", "名称", "数值")
	fmt.Println("-------------------------------")
	for key, value := range stats {
		fmt.Printf("%-20s | %v\n", key, value)
	}
	fmt.Println("-------------------------------")
}

// handleTxn 处理 TXN 命令
func handleTxn(cli *client.Client, args []string) {
	if len(args) == 0 {
		fmt.Println("用法: TXN IF ... THEN ... ELSE ... 或 TXN PUT foo 1 DEL bar")
		fmt.Println("输入 TXNHELP 查看详细帮助")
		return
	}
	cmdStr := strings.Join(args, " ")
	if strings.Contains(cmdStr, "IF") && strings.Contains(cmdStr, "THEN") {
		txnReq, err := parseTxnCommand(cmdStr)
		if err != nil {
			fmt.Println("事务解析错误:", err)
			return
		}
		resp, err := cli.Txn(txnReq)
		if err != nil {
			fmt.Println("事务执行错误:", err)
		} else {
			fmt.Printf("事务结果: %s\n", formatTxnResponse(resp))
		}
	} else {
		ops, err := parseOps(args)
		if err != nil {
			fmt.Println("批量操作解析错误:", err)
			return
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
	}
}
