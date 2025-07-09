package main

import (
	"bufio"
	"fmt"
	"kv/pkg/client"
	"kv/pkg/kvpb"
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

	printHelp()

	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print(">> ")
		input, _ := reader.ReadString('\n')
		cmd := strings.TrimSpace(input)
		if cmd == "exit" || cmd == "quit" || cmd == "EXIT" || cmd == "QUIT" {
			break
		}
		if cmd == "" {
			continue
		}

		if cmd == "TXN" {
			handleTxnMode(cli, reader)
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

// printHelp 打印帮助信息
func printHelp() {
	fmt.Println("分布式KV存储客户端")
	fmt.Println("支持的命令:")
	fmt.Println("  GET key                    - 获取值")
	fmt.Println("  PUT key value              - 设置值（永不过期）")
	fmt.Println("  PUTTTL key value ttl       - 设置值（指定TTL秒数）")
	fmt.Println("  DEL key                    - 删除键")
	fmt.Println("  TXN                        - 事务操作（条件执行+原子提交）")
	fmt.Println("  exit/quit                  - 退出")
	fmt.Println("示例:")
	fmt.Println("  PUT config_key config_value")
	fmt.Println("  PUTTTL cache_key cache_value 300")
	fmt.Println("  PUTTTL session_key session_data 7200")
	fmt.Println()
}

// handleTxnMode 处理事务模式
func handleTxnMode(cli *client.Client, reader *bufio.Reader) {
	printTxnHelp()

	for {
		fmt.Print("TXN>> ")
		input, _ := reader.ReadString('\n')
		cmd := strings.TrimSpace(input)

		if cmd == "back" || cmd == "BACK" {
			break
		}
		if cmd == "" {
			continue
		}

		// 支持无条件批量操作
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

		// 解析事务命令
		txnReq, err := parseTxnCommand(cmd)
		if err != nil {
			fmt.Println("事务解析错误:", err)
			continue
		}

		// 执行事务
		resp, err := cli.Txn(txnReq)
		if err != nil {
			fmt.Println("事务执行错误:", err)
		} else {
			fmt.Printf("事务结果: %s\n", formatTxnResponse(resp))
		}
	}
}

// printTxnHelp 打印事务模式帮助信息
func printTxnHelp() {
	fmt.Println("=== 事务模式 ===")
	fmt.Println("支持两种用法：")
	fmt.Println("1. 条件事务: IF <条件> THEN <成功操作> ELSE <失败操作>")
	fmt.Println("   条件格式: EXISTS key | VALUE key = value | VALUE key != value")
	fmt.Println("   操作格式: PUT key value | DEL key | GET key")
	fmt.Println("   示例:")
	fmt.Println("     IF EXISTS user:123 THEN PUT user:123:status active ELSE PUT user:123:status inactive")
	fmt.Println("     IF VALUE config:version = 1 THEN PUT config:version 2 ELSE GET config:version")
	fmt.Println()
	fmt.Println("2. 无条件批量原子操作: 直接输入一串操作")
	fmt.Println("   示例:")
	fmt.Println("     PUT foo 1 PUT bar 2 DEL baz")
	fmt.Println("   上述所有操作会作为一个原子事务批量提交")
	fmt.Println()
	fmt.Println("输入 'back' 返回主菜单")
	fmt.Println()
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
