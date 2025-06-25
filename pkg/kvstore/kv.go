package kvstore

import (
	"errors"
	"fmt"
	"kv/pkg/kvpb"
	"os"
	"path/filepath"
	"sync"

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
	}

	// 1. 先从快照恢复
	kv.RestoreFromSnapshot()
	// 2. 再重放WAL
	if err := kv.walManager.ReplayAllWALs(kv.store); err != nil {
		return nil, err
	}
	// ReplayAllWALs方法现在会智能地处理现有WAL文件
	// 如果最新WAL文件还有空间就继续使用，否则创建新文件
	return kv, nil
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
	for _, pair := range kvStore.Pairs {
		kv.store.Set(pair.Key, pair.Value)
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
	for _, pair := range kvStore.Pairs {
		kv.store.Set(pair.Key, pair.Value)
	}
	// 覆盖本地快照文件
	os.WriteFile(kv.snapshotPath, snapshot, 0644)
	return nil
}

func (kv *KV) Put(key, value string) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if _, exists := kv.store.Get(key); exists {
		return errors.New("key already exists")
	}

	logEntry := fmt.Sprintf("PUT %s %s", key, value)
	if err := kv.walManager.WriteLogEntry(logEntry); err != nil {
		return err
	}

	kv.store.Set(key, value)
	return nil
}

func (kv *KV) Get(key string) (string, error) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	val, ok := kv.store.Get(key)
	if !ok {
		return "", errors.New("key does not exist")
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

	return kv.walManager.Close()
}

func (kv *KV) SerializeState() ([]byte, error) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	kvStore := &kvpb.KVStore{}
	pairs := kv.store.Range("", "") // 获取所有键值对

	for _, pair := range pairs {
		kvStore.Pairs = append(kvStore.Pairs, &kvpb.KVPair{
			Key:   pair.Key,
			Value: pair.Value,
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
