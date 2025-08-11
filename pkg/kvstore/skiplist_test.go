package kvstore

import (
	"testing"
	"time"
)

func TestSkipListBasic(t *testing.T) {
	sl := NewSkipList()

	// 测试Put和Get
	sl.Put("key1", "value1", 0)
	sl.Put("key2", "value2", 0)

	val, err := sl.Get("key1", 0)
	if err != nil {
		t.Errorf("Get key1 failed: %v", err)
	}
	if val.Value != "value1" {
		t.Errorf("Expected value1, got %s", val.Value)
	}

	val, err = sl.Get("key2", 0)
	if err != nil {
		t.Errorf("Get key2 failed: %v", err)
	}
	if val.Value != "value2" {
		t.Errorf("Expected value2, got %s", val.Value)
	}
}

func TestSkipListDelete(t *testing.T) {
	sl := NewSkipList()

	// 添加键值对
	sl.Put("key1", "value1", 0)
	sl.Put("key2", "value2", 0)

	// 删除键
	revision, err := sl.Delete("key1")
	if err != nil {
		t.Errorf("Delete key1 failed: %v", err)
	}
	if revision <= 0 {
		t.Errorf("Expected positive revision, got %d", revision)
	}

	// 验证删除
	_, err = sl.Get("key1", 0)
	if err == nil {
		t.Error("Expected error for deleted key")
	}

	// 验证其他键仍然存在
	val, err := sl.Get("key2", 0)
	if err != nil {
		t.Errorf("Get key2 failed: %v", err)
	}
	if val.Value != "value2" {
		t.Errorf("Expected value2, got %s", val.Value)
	}
}

func TestSkipListTTL(t *testing.T) {
	sl := NewSkipList()

	// 添加带TTL的键值对
	sl.Put("key1", "value1", 1) // 1秒TTL

	// 立即获取应该成功
	val, err := sl.Get("key1", 0)
	if err != nil {
		t.Errorf("Get key1 failed: %v", err)
	}
	if val.Value != "value1" {
		t.Errorf("Expected value1, got %s", val.Value)
	}

	// 等待TTL过期
	time.Sleep(2 * time.Second)

	// 获取应该失败
	_, err = sl.Get("key1", 0)
	if err == nil {
		t.Error("Expected error for expired key")
	}
}

func TestSkipListMVCC(t *testing.T) {
	sl := NewSkipList()

	// 多次更新同一个键
	sl.Put("key1", "value1", 0)
	sl.Put("key1", "value2", 0)
	sl.Put("key1", "value3", 0)

	// 获取最新版本
	val, err := sl.Get("key1", 0)
	if err != nil {
		t.Errorf("Get latest version failed: %v", err)
	}
	if val.Value != "value3" {
		t.Errorf("Expected value3, got %s", val.Value)
	}

	// 获取历史版本
	val, err = sl.Get("key1", val.ModRev-1)
	if err != nil {
		t.Errorf("Get previous version failed: %v", err)
	}
	if val.Value != "value2" {
		t.Errorf("Expected value2, got %s", val.Value)
	}
}

func TestSkipListRange(t *testing.T) {
	sl := NewSkipList()

	// 添加多个键值对
	sl.Put("a", "value1", 0)
	sl.Put("b", "value2", 0)
	sl.Put("c", "value3", 0)
	sl.Put("d", "value4", 0)
	sl.Put("e", "value5", 0)

	// 范围查询
	results, _, err := sl.Range("b", "d", 0, 10)
	if err != nil {
		t.Errorf("Range query failed: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("Expected 2 results, got %d", len(results))
	}

	// 验证结果
	expected := []string{"b", "c"}
	for i, result := range results {
		if result.Key != expected[i] {
			t.Errorf("Expected key %s, got %s", expected[i], result.Key)
		}
	}
}

func TestSkipListHistory(t *testing.T) {
	sl := NewSkipList()

	// 多次更新同一个键
	sl.Put("key1", "value1", 0)
	sl.Put("key1", "value2", 0)
	sl.Put("key1", "value3", 0)

	// 获取历史
	history, err := sl.GetHistory("key1", 10)
	if err != nil {
		t.Errorf("GetHistory failed: %v", err)
	}
	if len(history) != 3 {
		t.Errorf("Expected 3 history entries, got %d", len(history))
	}

	// 验证历史顺序（从旧到新）
	expected := []string{"value1", "value2", "value3"}
	for i, entry := range history {
		if entry.Value != expected[i] {
			t.Errorf("Expected value %s, got %s", expected[i], entry.Value)
		}
	}
}

func TestSkipListCompact(t *testing.T) {
	sl := NewSkipList()

	// 添加多个版本
	sl.Put("key1", "value1", 0)
	sl.Put("key1", "value2", 0)
	sl.Put("key1", "value3", 0)
	sl.Put("key1", "value4", 0)

	// 压缩到版本2
	sl.Compact(2)

	// 验证旧版本被删除
	history, err := sl.GetHistory("key1", 10)
	if err != nil {
		t.Errorf("GetHistory after compact failed: %v", err)
	}
	if len(history) < 2 {
		t.Errorf("Expected at least 2 history entries after compact, got %d", len(history))
	}

	// 验证最新版本仍然存在
	val, err := sl.Get("key1", 0)
	if err != nil {
		t.Errorf("Get latest version after compact failed: %v", err)
	}
	if val.Value != "value4" {
		t.Errorf("Expected value4, got %s", val.Value)
	}
}

func TestSkipListStats(t *testing.T) {
	sl := NewSkipList()

	// 添加一些数据
	sl.Put("key1", "value1", 0)
	sl.Put("key2", "value2", 0)
	sl.Put("key3", "value3", 0)

	stats := sl.GetStats()

	// 验证统计信息
	if stats["total_keys"] != 3 {
		t.Errorf("Expected 3 total keys, got %v", stats["total_keys"])
	}

	if stats["current_revision"] == nil {
		t.Error("Expected current_revision in stats")
	}
}
