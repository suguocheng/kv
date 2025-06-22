package main

import (
	"fmt"
	"kv/pkg/kvstore"
	"os"
	"path/filepath"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Println("Usage: go run test_fixed_wal.go <nodeID>")
		os.Exit(1)
	}

	nodeID := os.Args[1]
	walDir := fmt.Sprintf("data/node%s/wal", nodeID)
	maxEntries := 5 // 每个文件最多5个条目，与快照阈值一致

	fmt.Printf("Testing fixed WAL functionality for node %s\n", nodeID)
	fmt.Printf("WAL directory: %s\n", walDir)
	fmt.Printf("Max entries per file: %d\n", maxEntries)

	// 创建KV存储
	kv, err := kvstore.NewKV(walDir, maxEntries)
	if err != nil {
		fmt.Printf("Failed to create KV store: %v\n", err)
		os.Exit(1)
	}
	defer kv.Close()

	// 添加测试数据，模拟PUT和DEL操作
	fmt.Println("\nAdding test data with PUT and DEL operations...")

	// 前5个操作：3个PUT，2个DEL
	operations := []struct {
		op    string
		key   string
		value string
	}{
		{"PUT", "key1", "value1"},
		{"PUT", "key2", "value2"},
		{"DEL", "key1", ""}, // 删除key1
		{"PUT", "key3", "value3"},
		{"DEL", "key2", ""}, // 删除key2
		// 第6-10个操作
		{"PUT", "key4", "value4"},
		{"PUT", "key5", "value5"},
		{"PUT", "key6", "value6"},
		{"DEL", "key4", ""}, // 删除key4
		{"PUT", "key7", "value7"},
		// 第11-15个操作
		{"PUT", "key8", "value8"},
		{"PUT", "key9", "value9"},
		{"DEL", "key5", ""}, // 删除key5
		{"PUT", "key10", "value10"},
		{"PUT", "key11", "value11"},
	}

	for i, op := range operations {
		switch op.op {
		case "PUT":
			if err := kv.Put(op.key, op.value); err != nil {
				fmt.Printf("Failed to put %s: %v\n", op.key, err)
			} else {
				fmt.Printf("PUT %s = %s (index %d)\n", op.key, op.value, i+1)
			}
		case "DEL":
			if err := kv.Delete(op.key); err != nil {
				fmt.Printf("Failed to delete %s: %v\n", op.key, err)
			} else {
				fmt.Printf("DEL %s (index %d)\n", op.key, i+1)
			}
		}

		// 每5个操作生成一次快照
		if (i+1)%5 == 0 {
			fmt.Printf("\n--- Generating snapshot at index %d ---\n", i+1)

			// 生成快照
			snapshot, err := kv.SerializeState()
			if err != nil {
				fmt.Printf("Failed to serialize state: %v\n", err)
				continue
			}
			fmt.Printf("Snapshot size: %d bytes\n", len(snapshot))

			// 清理WAL文件
			if err := kv.CleanupWALFiles(i + 1); err != nil {
				fmt.Printf("Failed to cleanup WAL files: %v\n", err)
			} else {
				fmt.Printf("Successfully cleaned up WAL files up to index %d\n", i+1)
			}

			// 显示当前状态
			stats := kv.GetWALStats()
			fmt.Printf("WAL files after cleanup: %d, Total entries: %d\n",
				stats["total_wal_files"], stats["total_entries"])

			// 列出剩余文件
			files, _ := os.ReadDir(walDir)
			fmt.Printf("Remaining WAL files: ")
			for _, file := range files {
				if !file.IsDir() && filepath.Ext(file.Name()) == ".log" {
					fmt.Printf("%s ", file.Name())
				}
			}
			fmt.Println()
		}
	}

	// 最终统计
	fmt.Println("\n=== Final Statistics ===")
	stats := kv.GetWALStats()
	fmt.Printf("Total WAL files: %d\n", stats["total_wal_files"])
	fmt.Printf("Total entries: %d\n", stats["total_entries"])
	fmt.Printf("Max entries per file: %d\n", stats["max_entries_per_file"])
	fmt.Printf("Current WAL entries: %d\n", stats["current_wal_entries"])

	// 列出最终文件
	fmt.Println("\nFinal WAL files:")
	files, err := os.ReadDir(walDir)
	if err != nil {
		fmt.Printf("Failed to read WAL directory: %v\n", err)
	} else {
		for _, file := range files {
			if !file.IsDir() && filepath.Ext(file.Name()) == ".log" {
				fmt.Printf("  %s\n", file.Name())
			}
		}
	}

	fmt.Println("\nTest completed!")
}
