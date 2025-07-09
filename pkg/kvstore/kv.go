package kvstore

import (
	"errors"
	"fmt"
	"kv/pkg/kvpb"
	"os"
	"path/filepath"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
)

// type KVStore interface {
// 	NewKV(filepath string) (*KV, error)
// 	Put(key string, value string) error
// 	Get(key string) (string, error)
// 	Delete(key string) error
// }

type KV struct {
	walManager        *WALManager
	store             *SkipList
	mu                sync.RWMutex
	snapshotPath      string // 快照文件路径
	lastSnapshotIndex int    // 最后快照的索引
	cleanupTicker     *time.Ticker
	stopCleanup       chan struct{}
}

func NewKV(walDir string, maxEntriesPerFile int) (*KV, error) {
	// 构造快照文件路径
	snapshotPath := filepath.Join(filepath.Dir(walDir), "snapshot.pb")

	// 创建WAL管理器
	walManager, err := NewWALManager(walDir, maxEntriesPerFile)
	if err != nil {
		return nil, err
	}

	kv := &KV{
		walManager:   walManager,
		store:        NewSkipList(),
		snapshotPath: snapshotPath,
		stopCleanup:  make(chan struct{}),
	}

	// 1. 先从快照恢复
	kv.RestoreFromSnapshot()
	// 2. 再重放WAL
	if err := kv.walManager.ReplayAllWALs(kv.store); err != nil {
		return nil, err
	}
	// ReplayAllWALs方法现在会智能地处理现有WAL文件
	// 如果最新WAL文件还有空间就继续使用，否则创建新文件

	// 启动定期清理过期键的goroutine
	kv.startCleanupRoutine()

	return kv, nil
}

// startCleanupRoutine 启动定期清理过期键的goroutine
func (kv *KV) startCleanupRoutine() {
	kv.cleanupTicker = time.NewTicker(30 * time.Second) // 每30秒清理一次
	go func() {
		for {
			select {
			case <-kv.cleanupTicker.C:
				kv.cleanupExpiredKeys()
			case <-kv.stopCleanup:
				return
			}
		}
	}()
}

// cleanupExpiredKeys 清理过期的键值对
func (kv *KV) cleanupExpiredKeys() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	cleaned := kv.store.CleanupExpired()
	if cleaned > 0 {
		fmt.Printf("Cleaned up %d expired keys\n", cleaned)
	}
}

// 从本地文件恢复快照
func (kv *KV) RestoreFromSnapshot() error {
	data, err := os.ReadFile(kv.snapshotPath)
	if os.IsNotExist(err) || len(data) == 0 {
		return nil
	}
	var kvStore kvpb.KVStore
	if err := proto.Unmarshal(data, &kvStore); err != nil {
		return err
	}

	restored := 0
	skipped := 0

	for _, pair := range kvStore.Pairs {
		ttl := int64(0)
		if pair.Ttl > 0 {
			ttl = pair.Ttl
		}

		// 检查是否过期（快照中的key没有时间戳，使用当前时间作为创建时间）
		if ttl > 0 {
			// 对于快照中的key，我们假设它们是在快照创建时写入的
			// 这里使用一个保守的策略：如果TTL很短（比如小于1小时），就跳过
			if ttl < 3600 { // 1小时
				fmt.Printf("Skipping potentially expired key from snapshot: %s (TTL: %d)\n", pair.Key, ttl)
				skipped++
				continue
			}
		}

		kv.store.Set(pair.Key, pair.Value, ttl)
		restored++
	}

	if restored > 0 || skipped > 0 {
		fmt.Printf("Snapshot restore: restored %d keys, skipped %d potentially expired keys\n", restored, skipped)
	}

	return nil
}

// 从快照数据恢复快照
func (kv *KV) RestoreFromSnapshotData(snapshot []byte) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if len(snapshot) == 0 {
		return nil
	}
	var kvStore kvpb.KVStore
	if err := proto.Unmarshal(snapshot, &kvStore); err != nil {
		return err
	}
	kv.store = NewSkipList()

	restored := 0
	skipped := 0

	for _, pair := range kvStore.Pairs {
		ttl := int64(0)
		if pair.Ttl > 0 {
			ttl = pair.Ttl
		}

		// 检查是否过期（快照中的key没有时间戳，使用保守策略）
		if ttl > 0 {
			// 对于快照中的key，如果TTL很短就跳过
			if ttl < 3600 { // 1小时
				fmt.Printf("Skipping potentially expired key from snapshot data: %s (TTL: %d)\n", pair.Key, ttl)
				skipped++
				continue
			}
		}

		kv.store.Set(pair.Key, pair.Value, ttl)
		restored++
	}

	if restored > 0 || skipped > 0 {
		fmt.Printf("Snapshot data restore: restored %d keys, skipped %d potentially expired keys\n", restored, skipped)
	}

	// 覆盖本地快照文件
	os.WriteFile(kv.snapshotPath, snapshot, 0644)
	return nil
}

func (kv *KV) Put(key, value string) error {
	return kv.PutWithTTL(key, value, 0) // 0表示永不过期
}

// PutWithTTL 设置键值对并指定TTL（秒）
func (kv *KV) PutWithTTL(key, value string, ttl int64) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if _, exists := kv.store.Get(key); exists {
		return errors.New("key already exists")
	}

	// 记录写入时间戳，格式：PUT key value ttl timestamp
	timestamp := time.Now().Unix()
	logEntry := fmt.Sprintf("PUT %s %s %d %d", key, value, ttl, timestamp)
	if err := kv.walManager.WriteLogEntry(logEntry); err != nil {
		return err
	}

	kv.store.Set(key, value, ttl)
	return nil
}

func (kv *KV) Get(key string) (string, error) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	val, ok := kv.store.Get(key)
	if !ok {
		return "", errors.New("key does not exist or has expired")
	}

	return val, nil
}

func (kv *KV) Delete(key string) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if _, exists := kv.store.Get(key); !exists {
		return errors.New("key does not exist")
	}

	logEntry := fmt.Sprintf("DEL %s", key)
	if err := kv.walManager.WriteLogEntry(logEntry); err != nil {
		return err
	}

	kv.store.Delete(key)
	return nil
}

func (kv *KV) Close() error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 停止清理goroutine
	if kv.cleanupTicker != nil {
		kv.cleanupTicker.Stop()
		close(kv.stopCleanup)
	}

	return kv.walManager.Close()
}

func (kv *KV) SerializeState() ([]byte, error) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	kvStore := &kvpb.KVStore{}
	pairs := kv.store.GetAllWithTTL() // 获取所有键值对（包括TTL）

	for _, pair := range pairs {
		kvStore.Pairs = append(kvStore.Pairs, &kvpb.KVPair{
			Key:   pair.Key,
			Value: pair.Value,
			Ttl:   pair.TTL,
		})
	}

	data, err := proto.Marshal(kvStore)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// CleanupWALFiles 清理已快照的WAL文件 - 使用快照覆盖点逻辑
func (kv *KV) CleanupWALFiles(snapshotIndex int) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 更新最后快照索引
	kv.lastSnapshotIndex = snapshotIndex

	return kv.walManager.CleanupWALFiles(snapshotIndex)
}

// GetWALStats 获取WAL统计信息
func (kv *KV) GetWALStats() map[string]interface{} {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	return kv.walManager.GetWALStats()
}

// GetWithTTL 获取键值对及其TTL信息
func (kv *KV) GetWithTTL(key string) (string, int64, error) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	val, ok := kv.store.Get(key)
	if !ok {
		return "", 0, errors.New("key does not exist or has expired")
	}

	// 获取TTL信息（这里需要从store中获取，暂时返回0）
	// TODO: 在SkipList中添加GetWithTTL方法
	return val, 0, nil
}
