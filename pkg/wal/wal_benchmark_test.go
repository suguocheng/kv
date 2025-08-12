package wal

import (
	"fmt"
	"os"
	"testing"
	"time"

	"kv/pkg/proto/kvpb"
)

// BenchmarkWALWrite 单线程WAL写入基准测试
func BenchmarkWALWrite(b *testing.B) {
	tempDir, err := os.MkdirTemp("", "wal_bench")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	walManager, err := NewWALManager(tempDir, 1000)
	if err != nil {
		b.Fatal(err)
	}
	defer walManager.Close()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		entry := &kvpb.WALEntry{
			Term:  1,
			Index: uint64(i + 1),
			Type:  kvpb.EntryType_ENTRY_NORMAL,
			Data:  []byte(fmt.Sprintf("benchmark_data_%d", i)),
		}

		err := walManager.WriteEntry(entry)
		if err != nil {
			b.Fatal(err)
		}
	}

	// 等待所有异步写入完成
	time.Sleep(100 * time.Millisecond)
}

// BenchmarkWALConcurrentWrite 并发WAL写入基准测试
func BenchmarkWALConcurrentWrite(b *testing.B) {
	tempDir, err := os.MkdirTemp("", "wal_concurrent_bench")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	walManager, err := NewWALManager(tempDir, 1000)
	if err != nil {
		b.Fatal(err)
	}
	defer walManager.Close()

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			entry := &kvpb.WALEntry{
				Term:  1,
				Index: uint64(i + 1),
				Type:  kvpb.EntryType_ENTRY_NORMAL,
				Data:  []byte(fmt.Sprintf("concurrent_benchmark_data_%d", i)),
			}

			err := walManager.WriteEntry(entry)
			if err != nil {
				b.Fatal(err)
			}
			i++
		}
	})

	// 等待所有异步写入完成
	time.Sleep(100 * time.Millisecond)
}

// BenchmarkWALSyncWrite 同步WAL写入基准测试
func BenchmarkWALSyncWrite(b *testing.B) {
	tempDir, err := os.MkdirTemp("", "wal_sync_bench")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	walManager, err := NewWALManager(tempDir, 1000)
	if err != nil {
		b.Fatal(err)
	}
	defer walManager.Close()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		entry := &kvpb.WALEntry{
			Term:  1,
			Index: uint64(i + 1),
			Type:  kvpb.EntryType_ENTRY_NORMAL,
			Data:  []byte(fmt.Sprintf("sync_benchmark_data_%d", i)),
		}

		err := walManager.WriteEntrySync(entry)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkWALLargeData 大数据WAL写入基准测试
func BenchmarkWALLargeData(b *testing.B) {
	tempDir, err := os.MkdirTemp("", "wal_large_bench")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	walManager, err := NewWALManager(tempDir, 1000)
	if err != nil {
		b.Fatal(err)
	}
	defer walManager.Close()

	// 创建1KB的数据
	largeData := make([]byte, 1024)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		entry := &kvpb.WALEntry{
			Term:  1,
			Index: uint64(i + 1),
			Type:  kvpb.EntryType_ENTRY_NORMAL,
			Data:  largeData,
		}

		err := walManager.WriteEntry(entry)
		if err != nil {
			b.Fatal(err)
		}
	}

	// 等待所有异步写入完成
	time.Sleep(100 * time.Millisecond)
}

// BenchmarkWALReplay WAL重放基准测试
func BenchmarkWALReplay(b *testing.B) {
	tempDir, err := os.MkdirTemp("", "wal_replay_bench")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	// 创建WAL并写入一些数据
	walManager, err := NewWALManager(tempDir, 1000)
	if err != nil {
		b.Fatal(err)
	}

	// 写入1000个条目
	for i := 0; i < 1000; i++ {
		entry := &kvpb.WALEntry{
			Term:  1,
			Index: uint64(i + 1),
			Type:  kvpb.EntryType_ENTRY_NORMAL,
			Data:  []byte(fmt.Sprintf("replay_data_%d", i)),
		}

		err := walManager.WriteEntry(entry)
		if err != nil {
			b.Fatal(err)
		}
	}

	// 等待写入完成
	time.Sleep(500 * time.Millisecond)
	walManager.Close()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// 创建新的WAL管理器进行重放
		replayWAL, err := NewWALManager(tempDir, 1000)
		if err != nil {
			b.Fatal(err)
		}

		// 重放WAL文件
		err = replayWAL.ReplayAllWALs(func(entry *kvpb.WALEntry) error {
			// 模拟处理逻辑
			_ = entry
			return nil
		}, 1)

		if err != nil {
			b.Fatal(err)
		}

		replayWAL.Close()
	}
}

// BenchmarkWALFileRotation WAL文件轮转基准测试
func BenchmarkWALFileRotation(b *testing.B) {
	tempDir, err := os.MkdirTemp("", "wal_rotation_bench")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	walManager, err := NewWALManager(tempDir, 100) // 较小的文件大小以触发轮转
	if err != nil {
		b.Fatal(err)
	}
	defer walManager.Close()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		entry := &kvpb.WALEntry{
			Term:  1,
			Index: uint64(i + 1),
			Type:  kvpb.EntryType_ENTRY_NORMAL,
			Data:  []byte(fmt.Sprintf("rotation_data_%d", i)),
		}

		err := walManager.WriteEntry(entry)
		if err != nil {
			b.Fatal(err)
		}
	}

	// 等待所有异步写入完成
	time.Sleep(100 * time.Millisecond)
}

// BenchmarkWALCleanup WAL清理基准测试
func BenchmarkWALCleanup(b *testing.B) {
	tempDir, err := os.MkdirTemp("", "wal_cleanup_bench")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	walManager, err := NewWALManager(tempDir, 100)
	if err != nil {
		b.Fatal(err)
	}

	// 写入足够的数据以创建多个文件
	for i := 0; i < 500; i++ {
		entry := &kvpb.WALEntry{
			Term:  1,
			Index: uint64(i + 1),
			Type:  kvpb.EntryType_ENTRY_NORMAL,
			Data:  []byte(fmt.Sprintf("cleanup_data_%d", i)),
		}

		err := walManager.WriteEntry(entry)
		if err != nil {
			b.Fatal(err)
		}
	}

	// 等待写入完成
	time.Sleep(500 * time.Millisecond)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// 清理到索引250
		err := walManager.CleanupWALFiles(250)
		if err != nil {
			b.Fatal(err)
		}
	}

	walManager.Close()
}

// BenchmarkWALMixedOperations 混合操作基准测试
func BenchmarkWALMixedOperations(b *testing.B) {
	tempDir, err := os.MkdirTemp("", "wal_mixed_bench")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	walManager, err := NewWALManager(tempDir, 1000)
	if err != nil {
		b.Fatal(err)
	}
	defer walManager.Close()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// 混合不同类型的操作
		operations := []struct {
			entryType kvpb.EntryType
			data      string
		}{
			{kvpb.EntryType_ENTRY_NORMAL, fmt.Sprintf("normal_data_%d", i)},
			{kvpb.EntryType_ENTRY_CONF_CHANGE, fmt.Sprintf("conf_data_%d", i)},
			{kvpb.EntryType_ENTRY_META, fmt.Sprintf("meta_data_%d", i)},
		}

		for j, op := range operations {
			entry := &kvpb.WALEntry{
				Term:  uint64(i + 1),
				Index: uint64(i*3 + j + 1),
				Type:  op.entryType,
				Data:  []byte(op.data),
			}

			err := walManager.WriteEntry(entry)
			if err != nil {
				b.Fatal(err)
			}
		}
	}

	// 等待所有异步写入完成
	time.Sleep(100 * time.Millisecond)
}
