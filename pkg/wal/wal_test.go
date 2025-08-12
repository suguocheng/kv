package wal

import (
	"fmt"
	"os"
	"testing"
	"time"

	"kv/pkg/proto/kvpb"
)

// 测试辅助函数
func setupTestWAL(t *testing.T) (*WALManager, string) {
	tempDir, err := os.MkdirTemp("", "wal_test")
	if err != nil {
		t.Fatal(err)
	}

	walManager, err := NewWALManager(tempDir, 1000)
	if err != nil {
		os.RemoveAll(tempDir)
		t.Fatal(err)
	}

	return walManager, tempDir
}

func cleanupTestWAL(walManager *WALManager, tempDir string) {
	if walManager != nil {
		walManager.Close()
	}
	if tempDir != "" {
		os.RemoveAll(tempDir)
	}
}

// ==================== 基础功能测试 ====================

func TestWALManagerCreation(t *testing.T) {
	walManager, tempDir := setupTestWAL(t)
	defer cleanupTestWAL(walManager, tempDir)

	// 验证WAL管理器创建成功
	if walManager == nil {
		t.Fatal("Expected WAL manager to be created")
	}

	// 验证基本属性
	if walManager.walDir != tempDir {
		t.Errorf("Expected walDir=%s, got %s", tempDir, walManager.walDir)
	}
	if walManager.maxEntries != 1000 {
		t.Errorf("Expected maxEntries=1000, got %d", walManager.maxEntries)
	}
	if walManager.bufferSize != 64*1024 {
		t.Errorf("Expected bufferSize=64KB, got %d", walManager.bufferSize)
	}
}

func TestWALWriteEntry(t *testing.T) {
	walManager, tempDir := setupTestWAL(t)
	defer cleanupTestWAL(walManager, tempDir)

	// 创建测试条目
	entry := &kvpb.WALEntry{
		Term:  1,
		Index: 1,
		Type:  kvpb.EntryType_ENTRY_NORMAL,
		Data:  []byte("test data"),
	}

	// 写入条目
	err := walManager.WriteEntry(entry)
	if err != nil {
		t.Errorf("WriteEntry failed: %v", err)
	}

	// 等待异步写入完成
	time.Sleep(200 * time.Millisecond)

	// 验证文件是否创建
	files, err := os.ReadDir(tempDir)
	if err != nil {
		t.Fatal(err)
	}

	if len(files) == 0 {
		t.Error("Expected WAL file to be created")
	}
}

func TestWALErrorHandling(t *testing.T) {
	walManager, tempDir := setupTestWAL(t)
	defer cleanupTestWAL(walManager, tempDir)

	// 测试空条目
	err := walManager.WriteEntry(nil)
	if err == nil {
		t.Error("Expected error for nil entry")
	}

	// 测试正常条目
	entry := &kvpb.WALEntry{
		Term:  1,
		Index: 1,
		Type:  kvpb.EntryType_ENTRY_NORMAL,
		Data:  []byte("test data"),
	}

	err = walManager.WriteEntry(entry)
	if err != nil {
		t.Errorf("WriteEntry failed: %v", err)
	}

	// 等待异步写入完成
	time.Sleep(100 * time.Millisecond)
}

func TestWALFileRotation(t *testing.T) {
	walManager, tempDir := setupTestWAL(t)
	defer cleanupTestWAL(walManager, tempDir)

	// 写入超过最大条目数的条目，触发文件轮转
	for i := 1; i <= 1500; i++ {
		entry := &kvpb.WALEntry{
			Term:  uint64(i),
			Index: uint64(i),
			Type:  kvpb.EntryType_ENTRY_NORMAL,
			Data:  []byte(fmt.Sprintf("test data %d", i)),
		}

		err := walManager.WriteEntry(entry)
		if err != nil {
			t.Errorf("WriteEntry %d failed: %v", i, err)
		}
	}

	// 等待异步写入完成
	time.Sleep(500 * time.Millisecond)

	// 验证文件命名格式
	files, err := os.ReadDir(tempDir)
	if err != nil {
		t.Fatal(err)
	}

	for _, file := range files {
		if !file.IsDir() {
			// 验证文件名格式：start-end.wal
			if len(file.Name()) < 8 || file.Name()[len(file.Name())-4:] != ".wal" {
				t.Errorf("Invalid WAL file name: %s", file.Name())
			}
		}
	}
}

func TestWALReplay(t *testing.T) {
	walManager, tempDir := setupTestWAL(t)
	defer cleanupTestWAL(walManager, tempDir)

	// 写入一些测试条目
	testData := []string{"data1", "data2", "data3", "data4", "data5"}
	for i, data := range testData {
		entry := &kvpb.WALEntry{
			Term:  uint64(i + 1),
			Index: uint64(i + 1),
			Type:  kvpb.EntryType_ENTRY_NORMAL,
			Data:  []byte(data),
		}

		err := walManager.WriteEntry(entry)
		if err != nil {
			t.Errorf("WriteEntry failed: %v", err)
		}
	}

	// 等待异步写入完成
	time.Sleep(200 * time.Millisecond)

	// 关闭WAL管理器
	walManager.Close()

	// 创建新的WAL管理器进行重放
	replayWAL, err := NewWALManager(tempDir, 1000)
	if err != nil {
		t.Fatal(err)
	}
	defer replayWAL.Close()

	// 重放WAL文件
	replayedData := make([]string, 0)
	err = replayWAL.ReplayAllWALs(func(entry *kvpb.WALEntry) error {
		replayedData = append(replayedData, string(entry.Data))
		return nil
	}, 1)

	if err != nil {
		t.Errorf("ReplayAllWALs failed: %v", err)
	}

	// 验证重放结果
	if len(replayedData) != len(testData) {
		t.Errorf("Expected %d entries, got %d", len(testData), len(replayedData))
	}

	for i, data := range testData {
		if replayedData[i] != data {
			t.Errorf("Expected %s, got %s", data, replayedData[i])
		}
	}
}

func TestWALReplayFromIndex(t *testing.T) {
	walManager, tempDir := setupTestWAL(t)
	defer cleanupTestWAL(walManager, tempDir)

	// 写入一些测试条目
	for i := 1; i <= 10; i++ {
		entry := &kvpb.WALEntry{
			Term:  uint64(i),
			Index: uint64(i),
			Type:  kvpb.EntryType_ENTRY_NORMAL,
			Data:  []byte(fmt.Sprintf("data_%d", i)),
		}

		err := walManager.WriteEntry(entry)
		if err != nil {
			t.Errorf("WriteEntry failed: %v", err)
		}
	}

	// 等待异步写入完成
	time.Sleep(200 * time.Millisecond)

	// 从索引5开始重放
	replayedData := make([]string, 0)
	err := walManager.ReplayAllWALs(func(entry *kvpb.WALEntry) error {
		replayedData = append(replayedData, string(entry.Data))
		return nil
	}, 5)

	if err != nil {
		t.Errorf("ReplayAllWALs failed: %v", err)
	}

	// 应该只重放索引5-10的条目
	expectedCount := 6
	if len(replayedData) != expectedCount {
		t.Errorf("Expected %d entries, got %d", expectedCount, len(replayedData))
	}
}

func TestWALCleanup(t *testing.T) {
	walManager, tempDir := setupTestWAL(t)
	defer cleanupTestWAL(walManager, tempDir)

	// 写入一些测试条目
	for i := 1; i <= 1000; i++ {
		entry := &kvpb.WALEntry{
			Term:  uint64(i),
			Index: uint64(i),
			Type:  kvpb.EntryType_ENTRY_NORMAL,
			Data:  []byte(fmt.Sprintf("data_%d", i)),
		}

		err := walManager.WriteEntry(entry)
		if err != nil {
			t.Errorf("WriteEntry failed: %v", err)
		}
	}

	// 等待异步写入完成
	time.Sleep(500 * time.Millisecond)

	// 清理到索引500
	err := walManager.CleanupWALFiles(500)
	if err != nil {
		t.Errorf("CleanupWALFiles failed: %v", err)
	}
}

func TestWALPersistence(t *testing.T) {
	walManager, tempDir := setupTestWAL(t)
	defer cleanupTestWAL(walManager, tempDir)

	// 写入一些测试条目
	testData := []string{"persistent_data_1", "persistent_data_2"}
	for i, data := range testData {
		entry := &kvpb.WALEntry{
			Term:  uint64(i + 1),
			Index: uint64(i + 1),
			Type:  kvpb.EntryType_ENTRY_NORMAL,
			Data:  []byte(data),
		}

		err := walManager.WriteEntry(entry)
		if err != nil {
			t.Errorf("WriteEntry failed: %v", err)
		}
	}

	// 等待异步写入完成
	time.Sleep(200 * time.Millisecond)

	// 关闭WAL管理器
	walManager.Close()

	// 验证文件确实被创建
	files, err := os.ReadDir(tempDir)
	if err != nil {
		t.Fatal(err)
	}

	if len(files) == 0 {
		t.Error("Expected WAL files to be created")
	}
}

func TestWALEntryTypes(t *testing.T) {
	walManager, tempDir := setupTestWAL(t)
	defer cleanupTestWAL(walManager, tempDir)

	// 测试不同类型的条目
	entryTypes := []kvpb.EntryType{
		kvpb.EntryType_ENTRY_NORMAL,
		kvpb.EntryType_ENTRY_CONF_CHANGE,
		kvpb.EntryType_ENTRY_META,
	}

	for i, entryType := range entryTypes {
		entry := &kvpb.WALEntry{
			Term:  uint64(i + 1),
			Index: uint64(i + 1),
			Type:  entryType,
			Data:  []byte(fmt.Sprintf("type_test_%d", i)),
		}

		err := walManager.WriteEntry(entry)
		if err != nil {
			t.Errorf("WriteEntry with type %v failed: %v", entryType, err)
		}
	}

	// 等待异步写入完成
	time.Sleep(200 * time.Millisecond)
}

func TestWALStats(t *testing.T) {
	walManager, tempDir := setupTestWAL(t)
	defer cleanupTestWAL(walManager, tempDir)

	// 写入一些测试条目
	for i := 1; i <= 100; i++ {
		entry := &kvpb.WALEntry{
			Term:  uint64(i),
			Index: uint64(i),
			Type:  kvpb.EntryType_ENTRY_NORMAL,
			Data:  []byte(fmt.Sprintf("stats_test_%d", i)),
		}

		err := walManager.WriteEntry(entry)
		if err != nil {
			t.Errorf("WriteEntry failed: %v", err)
		}
	}

	// 等待异步写入完成
	time.Sleep(200 * time.Millisecond)

	// 获取统计信息
	stats := walManager.GetStats()
	if stats == nil {
		t.Error("Expected stats to be returned")
	}

	// 验证统计信息包含必要的字段
	requiredFields := []string{"total_writes", "avg_latency", "flush_count"}
	for _, field := range requiredFields {
		if _, exists := stats[field]; !exists {
			t.Errorf("Expected stats to contain field: %s", field)
		}
	}
}

// ==================== 并发测试 ====================

func TestWALConcurrentWrites(t *testing.T) {
	walManager, tempDir := setupTestWAL(t)
	defer cleanupTestWAL(walManager, tempDir)

	// 并发写入测试
	numWriters := 10
	numWrites := 100
	done := make(chan bool, numWriters)

	for i := 0; i < numWriters; i++ {
		go func(writerID int) {
			for j := 0; j < numWrites; j++ {
				entry := &kvpb.WALEntry{
					Term:  uint64(writerID),
					Index: uint64(writerID*numWrites + j + 1),
					Type:  kvpb.EntryType_ENTRY_NORMAL,
					Data:  []byte(fmt.Sprintf("concurrent_data_%d_%d", writerID, j)),
				}

				err := walManager.WriteEntry(entry)
				if err != nil {
					t.Errorf("Concurrent WriteEntry failed: %v", err)
					return
				}
			}
			done <- true
		}(i)
	}

	// 等待所有写入完成
	for i := 0; i < numWriters; i++ {
		<-done
	}

	// 等待异步写入完成
	time.Sleep(500 * time.Millisecond)

	// 验证写入结果
	stats := walManager.GetStats()
	totalWrites := stats["total_writes"].(int64)
	expectedWrites := int64(numWriters * numWrites)

	if totalWrites != expectedWrites {
		t.Errorf("Expected %d writes, got %d", expectedWrites, totalWrites)
	}

	t.Logf("Concurrent writes completed: %d writes, avg latency: %v",
		totalWrites, stats["avg_latency"])
}

func TestWALBufferFlush(t *testing.T) {
	walManager, tempDir := setupTestWAL(t)
	defer cleanupTestWAL(walManager, tempDir)

	// 写入大量数据触发缓冲区刷盘
	largeData := make([]byte, 1024) // 1KB数据
	for i := 0; i < 100; i++ {
		entry := &kvpb.WALEntry{
			Term:  1,
			Index: uint64(i + 1),
			Type:  kvpb.EntryType_ENTRY_NORMAL,
			Data:  largeData,
		}

		err := walManager.WriteEntry(entry)
		if err != nil {
			t.Errorf("WriteEntry failed: %v", err)
		}
	}

	// 等待异步写入完成
	time.Sleep(500 * time.Millisecond)

	// 验证缓冲区刷盘统计
	stats := walManager.GetStats()
	flushCount := stats["flush_count"].(int64)
	bufferHits := stats["buffer_hits"].(int64)

	if flushCount == 0 {
		t.Error("Expected buffer flushes to occur")
	}

	t.Logf("Buffer flush test: %d flushes, %d buffer hits", flushCount, bufferHits)
}

func TestWALSyncWrite(t *testing.T) {
	walManager, tempDir := setupTestWAL(t)
	defer cleanupTestWAL(walManager, tempDir)

	// 同步写入测试
	entry := &kvpb.WALEntry{
		Term:  1,
		Index: 1,
		Type:  kvpb.EntryType_ENTRY_NORMAL,
		Data:  []byte("sync test data"),
	}

	err := walManager.WriteEntrySync(entry)
	if err != nil {
		t.Errorf("WriteEntrySync failed: %v", err)
	}

	// 验证文件是否立即写入
	files, err := os.ReadDir(tempDir)
	if err != nil {
		t.Fatal(err)
	}

	if len(files) == 0 {
		t.Error("Expected WAL file to be created immediately after sync write")
	}
}
