package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"kv/pkg/proto/kvpb"
)

// ==================== WAL功能测试 ====================
// 本文件包含WAL的功能测试，使用生产环境配置（1000条目/文件）
// 主要用于：
// 1. 验证WAL功能
// 2. 单元测试和CI/CD
// 3. 生产环境配置验证
// 4. 性能测试

func setupTestWAL(t *testing.T) (*WALManager, string) {
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "wal_test")
	if err != nil {
		t.Fatal(err)
	}

	// 创建WAL管理器 - 使用生产环境配置
	walManager, err := NewWALManager(tempDir, 1000)
	if err != nil {
		os.RemoveAll(tempDir)
		t.Fatal(err)
	}

	return walManager, tempDir
}

func cleanupTestWAL(walManager *WALManager, tempDir string) {
	if walManager != nil {
		// 关闭所有WAL文件
		for _, walFile := range walManager.walFiles {
			if walFile.file != nil {
				walFile.file.Close()
			}
		}
	}
	if tempDir != "" {
		os.RemoveAll(tempDir)
	}
}

func TestWALManagerCreation(t *testing.T) {
	walManager, tempDir := setupTestWAL(t)
	defer cleanupTestWAL(walManager, tempDir)

	// 验证WAL管理器创建成功
	if walManager == nil {
		t.Fatal("Expected WAL manager to be created")
	}

	if walManager.walDir == "" {
		t.Error("Expected WAL directory to be set")
	}

	if walManager.maxEntries != 1000 {
		t.Errorf("Expected maxEntries=1000, got %d", walManager.maxEntries)
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

	// 验证当前WAL文件存在
	if walManager.currentWAL == nil {
		t.Error("Expected current WAL file to be created")
	}

	// 验证条目计数
	if walManager.currentWAL.entries != 1 {
		t.Errorf("Expected 1 entry, got %d", walManager.currentWAL.entries)
	}
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

	// 验证创建了多个WAL文件
	if len(walManager.walFiles) < 2 {
		t.Errorf("Expected at least 2 WAL files, got %d", len(walManager.walFiles))
	}

	// 验证当前WAL文件的条目数不超过最大值
	if walManager.currentWAL.entries > walManager.maxEntries {
		t.Errorf("Expected current WAL entries <= %d, got %d",
			walManager.maxEntries, walManager.currentWAL.entries)
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

	// 记录重放的条目
	var replayedEntries []*kvpb.WALEntry
	applyFunc := func(entry *kvpb.WALEntry) error {
		replayedEntries = append(replayedEntries, entry)
		return nil
	}

	// 重放所有WAL文件
	err := walManager.ReplayAllWALs(applyFunc, 1)
	if err != nil {
		t.Errorf("ReplayAllWALs failed: %v", err)
	}

	// 验证重放的条目数量
	if len(replayedEntries) != len(testData) {
		t.Errorf("Expected %d replayed entries, got %d", len(testData), len(replayedEntries))
	}

	// 验证重放的条目内容
	for i, entry := range replayedEntries {
		if string(entry.Data) != testData[i] {
			t.Errorf("Expected data %s, got %s", testData[i], string(entry.Data))
		}
		if entry.Index != uint64(i+1) {
			t.Errorf("Expected index %d, got %d", i+1, entry.Index)
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
			Data:  []byte(fmt.Sprintf("data%d", i)),
		}

		err := walManager.WriteEntry(entry)
		if err != nil {
			t.Errorf("WriteEntry failed: %v", err)
		}
	}

	// 从索引5开始重放
	var replayedEntries []*kvpb.WALEntry
	applyFunc := func(entry *kvpb.WALEntry) error {
		replayedEntries = append(replayedEntries, entry)
		return nil
	}

	err := walManager.ReplayAllWALs(applyFunc, 5)
	if err != nil {
		t.Errorf("ReplayAllWALs from index 5 failed: %v", err)
	}

	// 验证只重放了索引5及以后的条目
	expectedCount := 6 // 索引5-10
	if len(replayedEntries) != expectedCount {
		t.Errorf("Expected %d replayed entries, got %d", expectedCount, len(replayedEntries))
	}

	// 验证第一个重放的条目索引是5
	if len(replayedEntries) > 0 && replayedEntries[0].Index != 5 {
		t.Errorf("Expected first replayed entry index 5, got %d", replayedEntries[0].Index)
	}
}

func TestWALCleanup(t *testing.T) {
	walManager, tempDir := setupTestWAL(t)
	defer cleanupTestWAL(walManager, tempDir)

	// 写入一些条目（超过1000个以触发文件轮转）
	for i := 1; i <= 1500; i++ {
		entry := &kvpb.WALEntry{
			Term:  uint64(i),
			Index: uint64(i),
			Type:  kvpb.EntryType_ENTRY_NORMAL,
			Data:  []byte(fmt.Sprintf("data%d", i)),
		}

		err := walManager.WriteEntry(entry)
		if err != nil {
			t.Errorf("WriteEntry failed: %v", err)
		}
	}

	// 记录清理前的文件数量
	initialFileCount := len(walManager.walFiles)
	fmt.Printf("Initial file count: %d\n", initialFileCount)

	// 清理到索引1500（清理第一个文件，它的EndIdx是1000）
	err := walManager.CleanupWALFiles(1500)
	if err != nil {
		t.Errorf("CleanupWALFiles failed: %v", err)
	}

	// 验证文件数量减少
	finalFileCount := len(walManager.walFiles)
	fmt.Printf("Final file count: %d\n", finalFileCount)
	if finalFileCount >= initialFileCount {
		t.Errorf("Expected WAL files to be cleaned up, initial: %d, final: %d", initialFileCount, finalFileCount)
	}
}

func TestWALPersistence(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "wal_persistence_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	// 创建第一个WAL管理器
	walManager1, err := NewWALManager(tempDir, 1000)
	if err != nil {
		t.Fatal(err)
	}

	// 写入一些条目
	testData := []string{"persist1", "persist2", "persist3"}
	for i, data := range testData {
		entry := &kvpb.WALEntry{
			Term:  uint64(i + 1),
			Index: uint64(i + 1),
			Type:  kvpb.EntryType_ENTRY_NORMAL,
			Data:  []byte(data),
		}

		err := walManager1.WriteEntry(entry)
		if err != nil {
			t.Errorf("WriteEntry failed: %v", err)
		}
	}

	// 关闭第一个WAL管理器
	cleanupTestWAL(walManager1, "")

	// 创建第二个WAL管理器，应该恢复数据
	walManager2, err := NewWALManager(tempDir, 1000)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanupTestWAL(walManager2, "")

	// 重放验证数据
	var replayedEntries []*kvpb.WALEntry
	applyFunc := func(entry *kvpb.WALEntry) error {
		replayedEntries = append(replayedEntries, entry)
		return nil
	}

	err = walManager2.ReplayAllWALs(applyFunc, 1)
	if err != nil {
		t.Errorf("ReplayAllWALs failed: %v", err)
	}

	// 验证数据恢复
	if len(replayedEntries) != len(testData) {
		t.Errorf("Expected %d replayed entries, got %d", len(testData), len(replayedEntries))
	}

	for i, entry := range replayedEntries {
		if string(entry.Data) != testData[i] {
			t.Errorf("Expected data %s, got %s", testData[i], string(entry.Data))
		}
	}
}

func TestWALConcurrentWrites(t *testing.T) {
	walManager, tempDir := setupTestWAL(t)
	defer cleanupTestWAL(walManager, tempDir)

	// 并发写入测试 - 使用顺序的Index避免冲突
	const numGoroutines = 3
	const numWritesPerGoroutine = 3
	done := make(chan bool, numGoroutines)

	// 使用原子计数器来生成唯一的Index，从1开始
	var indexCounter int32 = 1

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			for j := 0; j < numWritesPerGoroutine; j++ {
				// 使用原子操作生成唯一的Index
				index := atomic.AddInt32(&indexCounter, 1)

				entry := &kvpb.WALEntry{
					Term:  uint64(id*numWritesPerGoroutine + j + 1),
					Index: uint64(index),
					Type:  kvpb.EntryType_ENTRY_NORMAL,
					Data:  []byte(fmt.Sprintf("concurrent_data_%d_%d", id, j)),
				}

				err := walManager.WriteEntry(entry)
				if err != nil {
					t.Errorf("Concurrent WriteEntry failed: %v", err)
				}
			}
			done <- true
		}(i)
	}

	// 等待所有goroutine完成
	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	// 验证所有条目都被写入
	totalExpectedEntries := numGoroutines * numWritesPerGoroutine
	totalWrittenEntries := 0
	for i, walFile := range walManager.walFiles {
		totalWrittenEntries += walFile.entries
		t.Logf("WAL file %d: StartIdx=%d, EndIdx=%d, entries=%d", i, walFile.StartIdx, walFile.EndIdx, walFile.entries)
	}

	if totalWrittenEntries != totalExpectedEntries {
		t.Errorf("Expected %d total entries, got %d", totalExpectedEntries, totalWrittenEntries)
	}
}

func TestWALEntryTypes(t *testing.T) {
	walManager, tempDir := setupTestWAL(t)
	defer cleanupTestWAL(walManager, tempDir)

	// 测试不同类型的条目
	entryTypes := []kvpb.EntryType{
		kvpb.EntryType_ENTRY_NORMAL,
		kvpb.EntryType_ENTRY_CONF_CHANGE,
		kvpb.EntryType_ENTRY_NORMAL,
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

	// 重放验证条目类型
	var replayedEntries []*kvpb.WALEntry
	applyFunc := func(entry *kvpb.WALEntry) error {
		replayedEntries = append(replayedEntries, entry)
		return nil
	}

	err := walManager.ReplayAllWALs(applyFunc, 1)
	if err != nil {
		t.Errorf("ReplayAllWALs failed: %v", err)
	}

	// 验证条目类型
	for i, entry := range replayedEntries {
		if entry.Type != entryTypes[i] {
			t.Errorf("Expected entry type %v, got %v", entryTypes[i], entry.Type)
		}
	}
}

func TestWALErrorHandling(t *testing.T) {
	// 测试无效目录
	_, err := NewWALManager("/invalid/path/that/does/not/exist", 1000)
	if err == nil {
		t.Error("Expected error for invalid directory")
	}

	// 测试无效的条目
	walManager, tempDir := setupTestWAL(t)
	defer cleanupTestWAL(walManager, tempDir)

	// 测试nil条目
	err = walManager.WriteEntry(nil)
	if err == nil {
		t.Error("Expected error for nil entry")
	}

	// 先写入一些数据
	for i := 1; i <= 5; i++ {
		entry := &kvpb.WALEntry{
			Term:  uint64(i),
			Index: uint64(i),
			Type:  kvpb.EntryType_ENTRY_NORMAL,
			Data:  []byte(fmt.Sprintf("data%d", i)),
		}
		err := walManager.WriteEntry(entry)
		if err != nil {
			t.Errorf("WriteEntry failed: %v", err)
		}
	}

	// 测试无效的apply函数
	invalidApplyFunc := func(entry *kvpb.WALEntry) error {
		return fmt.Errorf("simulated error")
	}

	err = walManager.ReplayAllWALs(invalidApplyFunc, 1)
	if err == nil {
		t.Error("Expected error from invalid apply function")
	}
}

func TestWALFileNaming(t *testing.T) {
	walManager, tempDir := setupTestWAL(t)
	defer cleanupTestWAL(walManager, tempDir)

	// 写入足够多的条目触发文件轮转
	for i := 1; i <= 1500; i++ {
		entry := &kvpb.WALEntry{
			Term:  uint64(i),
			Index: uint64(i),
			Type:  kvpb.EntryType_ENTRY_NORMAL,
			Data:  []byte(fmt.Sprintf("data%d", i)),
		}

		err := walManager.WriteEntry(entry)
		if err != nil {
			t.Errorf("WriteEntry failed: %v", err)
		}
	}

	// 验证文件命名格式
	for _, walFile := range walManager.walFiles {
		base := filepath.Base(walFile.path)
		if !strings.HasSuffix(base, ".wal") {
			t.Errorf("Expected .wal suffix, got %s", base)
		}
	}
}

func TestWALStats(t *testing.T) {
	walManager, tempDir := setupTestWAL(t)
	defer cleanupTestWAL(walManager, tempDir)

	// 写入一些条目
	for i := 1; i <= 500; i++ {
		entry := &kvpb.WALEntry{
			Term:  uint64(i),
			Index: uint64(i),
			Type:  kvpb.EntryType_ENTRY_NORMAL,
			Data:  []byte(fmt.Sprintf("data%d", i)),
		}

		err := walManager.WriteEntry(entry)
		if err != nil {
			t.Errorf("WriteEntry failed: %v", err)
		}
	}

	// 验证统计信息
	if len(walManager.walFiles) == 0 {
		t.Error("Expected WAL files to exist")
	}

	if walManager.currentWAL == nil {
		t.Error("Expected current WAL file to exist")
	}

	// 验证文件信息
	for _, walFile := range walManager.walFiles {
		if walFile.StartIdx <= 0 {
			t.Error("Expected positive StartIdx")
		}
		if walFile.EndIdx < walFile.StartIdx {
			t.Errorf("Expected EndIdx >= StartIdx, got %d < %d", walFile.EndIdx, walFile.StartIdx)
		}
	}
}
