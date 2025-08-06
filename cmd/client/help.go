package main

import "fmt"

// printHelp 打印帮助信息
func printHelp() {
	fmt.Println("分布式 KV 存储客户端")
	fmt.Println("支持的命令:")
	fmt.Println("  基本操作:")
	fmt.Println("    GET key                         - 获取值")
	fmt.Println("    PUT key value                   - 设置值（永不过期）")
	fmt.Println("    PUTTTL key value ttl            - 设置值（指定TTL秒数）")
	fmt.Println("    DEL key                         - 删除键")
	fmt.Println("  事务操作:")
	fmt.Println("    IF <条件> THEN <操作> ELSE <操作> - 条件事务")
	fmt.Println("    条件格式: EXISTS key | VALUE key = value | VALUE key != value")
	fmt.Println("    操作格式: PUT key value | DEL key | GET key")
	fmt.Println("    TXN PUT foo 1 PUT bar 2 DEL baz     - 无条件批量原子操作")
	fmt.Println("  MVCC操作:")
	fmt.Println("    GETREV key revision             - 获取指定版本的值")
	fmt.Println("    HISTORY key [limit]             - 获取键的版本历史")
	fmt.Println("    RANGE start end [rev] [limit]   - 范围查询（支持版本）")
	fmt.Println("    COMPACT revision                - 压缩版本历史")
	fmt.Println("    STATS                           - 获取统计信息")
	fmt.Println("  Watch操作:")
	fmt.Println("    WATCH key|prefix [id]           - 监听键或前缀的变化")
	fmt.Println("    UNWATCH watcher_id              - 取消监听")
	fmt.Println("    WATCHSTATS                      - 获取Watch统计信息")
	fmt.Println("  其他:")
	fmt.Println("    help                            - 帮助信息")
	fmt.Println("    exit/quit                       - 退出")
	fmt.Println()
}
