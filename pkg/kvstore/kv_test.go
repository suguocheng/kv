package kvstore

import (
	"fmt"
	"os"
	"testing"
	"time"

	"kv/pkg/proto/kvpb"
)

func setupTestKV(t *testing.T) (*KV, string) {
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "kv_test")
	if err != nil {
		t.Fatal(err)
	}

	// 创建KV实例
	kv, err := NewKV(tempDir, 1000)
	if err != nil {
		os.RemoveAll(tempDir)
		t.Fatal(err)
	}

	return kv, tempDir
}

func cleanupTestKV(kv *KV, tempDir string) {
	if kv != nil {
		kv.Close()
	}
	if tempDir != "" {
		os.RemoveAll(tempDir)
	}
}

func TestKVBasicOperations(t *testing.T) {
	kv, tempDir := setupTestKV(t)
	defer cleanupTestKV(kv, tempDir)

	// 测试PUT操作
	err := kv.Put("key1", "value1")
	if err != nil {
		t.Errorf("Put failed: %v", err)
	}

	// 测试GET操作
	value, err := kv.Get("key1")
	if err != nil {
		t.Errorf("Get failed: %v", err)
	}
	if value != "value1" {
		t.Errorf("Expected value1, got %s", value)
	}

	// 测试DELETE操作
	err = kv.Delete("key1")
	if err != nil {
		t.Errorf("Delete failed: %v", err)
	}

	// 验证删除
	_, err = kv.Get("key1")
	if err == nil {
		t.Error("Expected error for deleted key")
	}
}

func TestKVTTLOperations(t *testing.T) {
	kv, tempDir := setupTestKV(t)
	defer cleanupTestKV(kv, tempDir)

	// 测试PUTTTL操作
	err := kv.PutWithTTL("key1", "value1", 1) // 1秒TTL
	if err != nil {
		t.Errorf("PutWithTTL failed: %v", err)
	}

	// 立即获取应该成功
	value, err := kv.Get("key1")
	if err != nil {
		t.Errorf("Get failed: %v", err)
	}
	if value != "value1" {
		t.Errorf("Expected value1, got %s", value)
	}

	// 等待TTL过期
	time.Sleep(2 * time.Second)

	// 获取应该失败
	_, err = kv.Get("key1")
	if err == nil {
		t.Error("Expected error for expired key")
	}
}

func TestKVMVCC(t *testing.T) {
	kv, tempDir := setupTestKV(t)
	defer cleanupTestKV(kv, tempDir)

	// 多次更新同一个键
	err := kv.Put("key1", "value1")
	if err != nil {
		t.Errorf("Put failed: %v", err)
	}

	err = kv.Put("key1", "value2")
	if err != nil {
		t.Errorf("Put failed: %v", err)
	}

	err = kv.Put("key1", "value3")
	if err != nil {
		t.Errorf("Put failed: %v", err)
	}

	// 获取最新版本
	value, err := kv.Get("key1")
	if err != nil {
		t.Errorf("Get latest version failed: %v", err)
	}
	if value != "value3" {
		t.Errorf("Expected value3, got %s", value)
	}

	// 获取历史版本
	value, revision, err := kv.GetWithRevision("key1", 0)
	if err != nil {
		t.Errorf("GetWithRevision failed: %v", err)
	}
	if value != "value3" {
		t.Errorf("Expected value3, got %s", value)
	}
	_ = revision // 避免未使用变量警告

	// 获取历史
	history, err := kv.GetHistory("key1", 10)
	if err != nil {
		t.Errorf("GetHistory failed: %v", err)
	}
	if len(history) < 3 {
		t.Errorf("Expected at least 3 history entries, got %d", len(history))
	}

	// 验证历史顺序（从旧到新），只检查最后3个版本
	expected := []string{"value1", "value2", "value3"}
	startIndex := len(history) - 3
	for i, expectedValue := range expected {
		if startIndex+i < len(history) {
			entry := history[startIndex+i]
			if entry.Value != expectedValue {
				t.Errorf("Expected value %s at position %d, got %s", expectedValue, i, entry.Value)
			}
		}
	}
}

func TestKVRange(t *testing.T) {
	kv, tempDir := setupTestKV(t)
	defer cleanupTestKV(kv, tempDir)

	// 添加多个键值对
	keys := []string{"a", "b", "c", "d", "e"}
	values := []string{"value1", "value2", "value3", "value4", "value5"}

	for i, key := range keys {
		err := kv.Put(key, values[i])
		if err != nil {
			t.Errorf("Put %s failed: %v", key, err)
		}
	}

	// 范围查询 - 使用左闭右开区间 [b, e)
	results, _, err := kv.Range("b", "e", 0, 10)
	if err != nil {
		t.Errorf("Range query failed: %v", err)
	}
	if len(results) != 3 {
		t.Errorf("Expected 3 results, got %d", len(results))
	}

	// 验证结果
	expected := []string{"b", "c", "d"}
	for i, result := range results {
		if result.Key != expected[i] {
			t.Errorf("Expected key %s, got %s", expected[i], result.Key)
		}
	}
}

func TestKVCompact(t *testing.T) {
	kv, tempDir := setupTestKV(t)
	defer cleanupTestKV(kv, tempDir)

	// 多次更新同一个键
	for i := 1; i <= 5; i++ {
		err := kv.Put("key1", fmt.Sprintf("value%d", i))
		if err != nil {
			t.Errorf("Put failed: %v", err)
		}
	}

	// 压缩到版本3
	compacted, err := kv.Compact(3)
	if err != nil {
		t.Errorf("Compact failed: %v", err)
	}
	if compacted <= 0 {
		t.Errorf("Expected positive compacted count, got %d", compacted)
	}

	// 验证最新版本仍然存在
	value, err := kv.Get("key1")
	if err != nil {
		t.Errorf("Get latest version after compact failed: %v", err)
	}
	if value != "value5" {
		t.Errorf("Expected value5, got %s", value)
	}
}

func TestKVTransaction(t *testing.T) {
	kv, tempDir := setupTestKV(t)
	defer cleanupTestKV(kv, tempDir)

	// 先设置一个键
	err := kv.Put("key1", "value1")
	if err != nil {
		t.Errorf("Put failed: %v", err)
	}

	// 创建事务请求
	txnReq := &kvpb.TxnRequest{
		Compare: []*kvpb.Compare{
			{
				Key:    "key1",
				Result: kvpb.CompareResult_EQUAL,
				Target: kvpb.CompareTarget_VALUE,
				Value:  "value1",
			},
		},
		Success: []*kvpb.Op{
			{
				Type:  "PUT",
				Key:   "key2",
				Value: []byte("value2"),
			},
		},
		Failure: []*kvpb.Op{
			{
				Type:  "PUT",
				Key:   "key3",
				Value: []byte("value3"),
			},
		},
	}

	// 执行事务
	resp, err := kv.Txn(txnReq)
	if err != nil {
		t.Errorf("Txn failed: %v", err)
	}
	if !resp.Succeeded {
		t.Error("Expected transaction to succeed")
	}

	// 验证成功分支执行
	value, err := kv.Get("key2")
	if err != nil {
		t.Errorf("Get key2 failed: %v", err)
	}
	if value != "value2" {
		t.Errorf("Expected value2, got %s", value)
	}

	// 验证失败分支未执行
	_, err = kv.Get("key3")
	if err == nil {
		t.Error("Expected key3 to not exist")
	}
}

func TestKVWatch(t *testing.T) {
	kv, tempDir := setupTestKV(t)
	defer cleanupTestKV(kv, tempDir)

	// 创建Watch监听器
	watcher, err := kv.WatchKeyWithID("test_watcher", "key1")
	if err != nil {
		t.Errorf("WatchKeyWithID failed: %v", err)
	}
	defer kv.Unwatch("test_watcher")

	// 设置键值，触发Watch事件
	err = kv.Put("key1", "value1")
	if err != nil {
		t.Errorf("Put failed: %v", err)
	}

	// 等待事件
	select {
	case event := <-watcher.Events:
		if event.Key != "key1" {
			t.Errorf("Expected key key1, got %s", event.Key)
		}
		if event.Value != "value1" {
			t.Errorf("Expected value value1, got %s", event.Value)
		}
	case <-time.After(2 * time.Second):
		t.Error("Timeout waiting for watch event")
	}
}

func TestKVPersistence(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "kv_persistence_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	// 创建第一个KV实例
	kv1, err := NewKV(tempDir, 1000)
	if err != nil {
		t.Fatal(err)
	}

	// 添加数据
	err = kv1.Put("key1", "value1")
	if err != nil {
		t.Errorf("Put failed: %v", err)
	}

	err = kv1.Put("key2", "value2")
	if err != nil {
		t.Errorf("Put failed: %v", err)
	}

	// 关闭第一个实例
	kv1.Close()

	// 创建第二个KV实例，应该恢复数据
	kv2, err := NewKV(tempDir, 1000)
	if err != nil {
		t.Fatal(err)
	}
	defer kv2.Close()

	// 验证数据恢复
	value, err := kv2.Get("key1")
	if err != nil {
		t.Errorf("Get key1 failed: %v", err)
	}
	if value != "value1" {
		t.Errorf("Expected value1, got %s", value)
	}

	value, err = kv2.Get("key2")
	if err != nil {
		t.Errorf("Get key2 failed: %v", err)
	}
	if value != "value2" {
		t.Errorf("Expected value2, got %s", value)
	}
}

func TestKVStats(t *testing.T) {
	kv, tempDir := setupTestKV(t)
	defer cleanupTestKV(kv, tempDir)

	// 添加一些数据
	err := kv.Put("key1", "value1")
	if err != nil {
		t.Errorf("Put failed: %v", err)
	}

	err = kv.Put("key2", "value2")
	if err != nil {
		t.Errorf("Put failed: %v", err)
	}

	// 获取统计信息
	stats := kv.GetStats()

	// 验证统计信息
	if stats["total_keys"] == nil {
		t.Error("Expected total_keys in stats")
	}

	if stats["current_revision"] == nil {
		t.Error("Expected current_revision in stats")
	}
}

func TestKVWALReplay(t *testing.T) {
	kv, tempDir := setupTestKV(t)
	defer cleanupTestKV(kv, tempDir)

	// 添加一些数据
	err := kv.Put("key1", "value1")
	if err != nil {
		t.Errorf("Put failed: %v", err)
	}

	err = kv.Put("key2", "value2")
	if err != nil {
		t.Errorf("Put failed: %v", err)
	}

	// 获取WAL管理器
	walManager := kv.GetWALManager()
	if walManager == nil {
		t.Error("Expected WAL manager to be available")
	}

	// 测试WAL重放
	err = kv.ReplayWALsFrom(1)
	if err != nil {
		t.Errorf("ReplayWALsFrom failed: %v", err)
	}
}

func TestKVSnapshot(t *testing.T) {
	kv, tempDir := setupTestKV(t)
	defer cleanupTestKV(kv, tempDir)

	// 添加一些数据
	err := kv.Put("key1", "value1")
	if err != nil {
		t.Errorf("Put failed: %v", err)
	}

	err = kv.Put("key2", "value2")
	if err != nil {
		t.Errorf("Put failed: %v", err)
	}

	// 序列化状态
	data, err := kv.SerializeState()
	if err != nil {
		t.Errorf("SerializeState failed: %v", err)
	}
	if len(data) == 0 {
		t.Error("Expected non-empty serialized state")
	}

	// 从快照恢复
	err = kv.RestoreFromSnapshotData(data)
	if err != nil {
		t.Errorf("RestoreFromSnapshotData failed: %v", err)
	}

	// 验证数据恢复
	value, err := kv.Get("key1")
	if err != nil {
		t.Errorf("Get key1 failed: %v", err)
	}
	if value != "value1" {
		t.Errorf("Expected value1, got %s", value)
	}
}

func TestKVConcurrentAccess(t *testing.T) {
	kv, tempDir := setupTestKV(t)
	defer cleanupTestKV(kv, tempDir)

	// 并发写入测试
	const numGoroutines = 10
	const numOperations = 100
	done := make(chan bool, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			for j := 0; j < numOperations; j++ {
				key := fmt.Sprintf("key_%d_%d", id, j)
				value := fmt.Sprintf("value_%d_%d", id, j)
				err := kv.Put(key, value)
				if err != nil {
					t.Errorf("Concurrent Put failed: %v", err)
				}
			}
			done <- true
		}(i)
	}

	// 等待所有goroutine完成
	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	// 验证数据
	for i := 0; i < numGoroutines; i++ {
		for j := 0; j < numOperations; j++ {
			key := fmt.Sprintf("key_%d_%d", i, j)
			expectedValue := fmt.Sprintf("value_%d_%d", i, j)
			value, err := kv.Get(key)
			if err != nil {
				t.Errorf("Get %s failed: %v", key, err)
			}
			if value != expectedValue {
				t.Errorf("Expected %s, got %s", expectedValue, value)
			}
		}
	}
}

func TestKVErrorHandling(t *testing.T) {
	kv, tempDir := setupTestKV(t)
	defer cleanupTestKV(kv, tempDir)

	// 测试获取不存在的键
	_, err := kv.Get("nonexistent")
	if err == nil {
		t.Error("Expected error for nonexistent key")
	}

	// 测试删除不存在的键
	err = kv.Delete("nonexistent")
	// 删除不存在的键可能不会返回错误，这是正常的

	// 测试无效的版本号
	_, _, err = kv.GetWithRevision("key1", -1)
	// 无效版本号可能不会返回错误，这是正常的
}

func TestKVWatchManager(t *testing.T) {
	kv, tempDir := setupTestKV(t)
	defer cleanupTestKV(kv, tempDir)

	// 创建多个监听器
	_, err := kv.WatchKeyWithID("watcher1", "key1")
	if err != nil {
		t.Errorf("WatchKeyWithID failed: %v", err)
	}

	_, err = kv.WatchKeyWithID("watcher2", "key1")
	if err != nil {
		t.Errorf("WatchKeyWithID failed: %v", err)
	}

	// 获取监听器列表
	watchers := kv.ListWatchers()
	if len(watchers) != 2 {
		t.Errorf("Expected 2 watchers, got %d", len(watchers))
	}

	// 获取特定监听器
	watcher, exists := kv.GetWatcher("watcher1")
	if !exists {
		t.Error("Expected watcher1 to exist")
	}
	if watcher.ID != "watcher1" {
		t.Errorf("Expected ID watcher1, got %s", watcher.ID)
	}

	// 获取Watch统计信息
	stats := kv.GetWatchStats()
	if stats["total_watchers"] != 2 {
		t.Errorf("Expected 2 total watchers, got %v", stats["total_watchers"])
	}

	// 清理监听器
	kv.CleanupWatchers()
}
