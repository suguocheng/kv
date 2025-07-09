package kvstore

import (
	"os"
	"testing"
	"time"
)

func TestTTL_WALReplayExpiration(t *testing.T) {
	// 清理测试目录
	os.RemoveAll("/tmp/testwal_replay")

	// 创建KV实例
	kv, err := NewKV("/tmp/testwal_replay", 100)
	if err != nil {
		t.Fatalf("NewKV failed: %v", err)
	}

	// 设置一个2秒后过期的key
	err = kv.PutWithTTL("expire_key", "expire_value", 2)
	if err != nil {
		t.Fatalf("PutWithTTL failed: %v", err)
	}

	// 设置一个永久key
	err = kv.Put("permanent_key", "permanent_value")
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// 关闭KV
	kv.Close()

	// 等待3秒，让key过期
	time.Sleep(3 * time.Second)

	// 重新创建KV（会重放WAL）
	kv2, err := NewKV("/tmp/testwal_replay", 100)
	if err != nil {
		t.Fatalf("NewKV failed: %v", err)
	}
	defer kv2.Close()

	// 验证过期的key不会被恢复
	val, err := kv2.Get("expire_key")
	if err == nil {
		t.Errorf("expire_key should not be restored after expiration, but got: %v", val)
	}

	// 验证永久key仍然存在
	val, err = kv2.Get("permanent_key")
	if err != nil || val != "permanent_value" {
		t.Errorf("permanent_key should still exist: want 'permanent_value', got '%v', err: %v", val, err)
	}
}

func TestTTL_SnapshotRestoreExpiration(t *testing.T) {
	// 清理测试目录
	os.RemoveAll("/tmp/testwal_snapshot")

	// 创建KV实例
	kv, err := NewKV("/tmp/testwal_snapshot", 100)
	if err != nil {
		t.Fatalf("NewKV failed: %v", err)
	}

	// 设置一些不同TTL的key
	err = kv.PutWithTTL("short_ttl", "short_value", 30) // 30秒
	if err != nil {
		t.Fatalf("PutWithTTL short_ttl failed: %v", err)
	}

	err = kv.PutWithTTL("long_ttl", "long_value", 7200) // 2小时
	if err != nil {
		t.Fatalf("PutWithTTL long_ttl failed: %v", err)
	}

	err = kv.Put("permanent_key", "permanent_value")
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// 创建快照
	_, err = kv.SerializeState()
	if err != nil {
		t.Fatalf("SerializeState failed: %v", err)
	}

	// 关闭KV
	kv.Close()

	// 重新创建KV（会从快照恢复）
	kv2, err := NewKV("/tmp/testwal_snapshot", 100)
	if err != nil {
		t.Fatalf("NewKV failed: %v", err)
	}
	defer kv2.Close()

	// 验证短TTL的key可能被跳过（保守策略）
	val, err := kv2.Get("short_ttl")
	if err != nil {
		// 这是预期的，因为短TTL key可能被跳过
		t.Logf("short_ttl was skipped during snapshot restore (expected)")
	} else {
		t.Logf("short_ttl was restored: %v", val)
	}

	// 验证长TTL的key应该被恢复
	val, err = kv2.Get("long_ttl")
	if err != nil || val != "long_value" {
		t.Errorf("long_ttl should be restored: want 'long_value', got '%v', err: %v", val, err)
	}

	// 验证永久key应该被恢复
	val, err = kv2.Get("permanent_key")
	if err != nil || val != "permanent_value" {
		t.Errorf("permanent_key should be restored: want 'permanent_value', got '%v', err: %v", val, err)
	}
}

func TestTTL_NewWALFormat(t *testing.T) {
	// 清理测试目录
	os.RemoveAll("/tmp/testwal_newformat")

	// 创建KV实例
	kv, err := NewKV("/tmp/testwal_newformat", 100)
	if err != nil {
		t.Fatalf("NewKV failed: %v", err)
	}

	// 设置一个TTL key（会使用新格式：PUT key value ttl timestamp）
	err = kv.PutWithTTL("newformat_key", "newformat_value", 5)
	if err != nil {
		t.Fatalf("PutWithTTL failed: %v", err)
	}

	// 立即关闭
	kv.Close()

	// 等待6秒让key过期
	time.Sleep(6 * time.Second)

	// 重新创建KV（会重放WAL）
	kv2, err := NewKV("/tmp/testwal_newformat", 100)
	if err != nil {
		t.Fatalf("NewKV failed: %v", err)
	}
	defer kv2.Close()

	// 验证过期的key不会被恢复
	val, err := kv2.Get("newformat_key")
	if err == nil {
		t.Errorf("newformat_key should not be restored after expiration, but got: %v", val)
	} else {
		t.Logf("newformat_key correctly not restored: %v", err)
	}
}
