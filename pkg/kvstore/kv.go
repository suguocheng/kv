package kvstore

import (
	"encoding/base64"
	"fmt"
	"kv/pkg/kvpb"
	"kv/pkg/watch"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
)

type KV struct {
	walManager        *WALManager
	store             *SkipList // 改为MVCC跳表
	mu                sync.RWMutex
	snapshotPath      string // 快照文件路径
	lastSnapshotIndex int    // 最后快照的索引
	cleanupTicker     *time.Ticker
	stopCleanup       chan struct{}
	watcher           *watch.KVWatcher // 添加Watch功能
	LastAppliedIndex  int              // WAL重放后，记录最后已应用的raft日志索引
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
		store:        NewSkipList(), // 使用MVCC跳表
		snapshotPath: snapshotPath,
		stopCleanup:  make(chan struct{}),
		watcher:      watch.NewKVWatcher(), // 初始化Watch功能
	}

	// 1. 先从快照恢复
	_ = kv.RestoreFromSnapshot()
	fromIdx := uint64(1)
	// 2. 自动重放WAL，恢复未快照的内容
	walManager.ReplayAllWALs(kv.ApplyWALEntry, fromIdx)

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

	// 获取过期的键值对
	expiredKeys := kv.store.GetExpiredKeys()

	// 清理过期键
	cleaned := kv.store.CleanupExpired()
	if cleaned > 0 {
		fmt.Printf("Cleaned up %d expired keys\n", cleaned)

		// 通知Watch监听器过期事件
		for _, key := range expiredKeys {
			kv.watcher.NotifyExpire(key, 0) // 过期事件使用0作为revision
		}
	}
}

// 从本地文件恢复快照
func (kv *KV) RestoreFromSnapshot() error {
	data, err := os.ReadFile(kv.snapshotPath)
	if os.IsNotExist(err) || len(data) == 0 {
		return nil
	}
	var mvccStore kvpb.MVCCStore
	if err := proto.Unmarshal(data, &mvccStore); err != nil {
		return err
	}
	kv.store = NewSkipList()
	for _, pair := range mvccStore.Pairs {
		kv.store.RestoreVersion(&VersionedKV{
			Key:        pair.Key,
			Value:      pair.Value,
			TTL:        pair.Ttl,
			CreatedRev: pair.CreatedRevision,
			ModRev:     pair.ModRevision,
			Version:    pair.Version,
			Deleted:    pair.Deleted,
			CreatedAt:  0, // 可选: 若有CreatedAt字段可补充
		})
	}
	kv.store.currentRevision = mvccStore.CurrentRevision
	return nil
}

// 从快照数据恢复快照
func (kv *KV) RestoreFromSnapshotData(snapshot []byte) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if len(snapshot) == 0 {
		return nil
	}
	var mvccStore kvpb.MVCCStore
	if err := proto.Unmarshal(snapshot, &mvccStore); err != nil {
		return err
	}
	kv.store = NewSkipList()
	for _, pair := range mvccStore.Pairs {
		kv.store.RestoreVersion(&VersionedKV{
			Key:        pair.Key,
			Value:      pair.Value,
			TTL:        pair.Ttl,
			CreatedRev: pair.CreatedRevision,
			ModRev:     pair.ModRevision,
			Version:    pair.Version,
			Deleted:    pair.Deleted,
			CreatedAt:  0, // 可选: 若有CreatedAt字段可补充
		})
	}
	kv.store.currentRevision = mvccStore.CurrentRevision
	// 覆盖本地快照文件
	os.WriteFile(kv.snapshotPath, snapshot, 0644)
	return nil
}

// Put/PutWithTTL/Delete等方法只做业务apply，不再写WAL
func (kv *KV) Put(key, value string) error {
	fmt.Printf("[KV] Put: key=%s value=%s\n", key, value)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, err := kv.store.Put(key, value, 0)
	return err
}

func (kv *KV) PutWithTTL(key, value string, ttl int64) error {
	fmt.Printf("[KV] PutWithTTL: key=%s value=%s ttl=%d\n", key, value, ttl)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, err := kv.store.Put(key, value, ttl)
	return err
}

func (kv *KV) Get(key string) (string, error) {
	versionedKV, err := kv.store.Get(key, 0)
	if err != nil {
		return "", err
	}
	return versionedKV.Value, nil
}

func (kv *KV) Delete(key string) error {
	fmt.Printf("[KV] Delete: key=%s\n", key)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, err := kv.store.Delete(key)
	return err
}

func (kv *KV) Close() error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 停止清理goroutine
	if kv.cleanupTicker != nil {
		kv.cleanupTicker.Stop()
		close(kv.stopCleanup)
	}

	return nil
}

func (kv *KV) SerializeState() ([]byte, error) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	mvccStore := &kvpb.MVCCStore{}
	nodes := kv.store.AllNodes()
	for _, node := range nodes {
		for _, version := range node.versions {
			mvccStore.Pairs = append(mvccStore.Pairs, &kvpb.VersionedKVPair{
				Key:             version.Key,
				Value:           version.Value,
				Ttl:             version.TTL,
				CreatedRevision: version.CreatedRev,
				ModRevision:     version.ModRev,
				Version:         version.Version,
				Deleted:         version.Deleted,
			})
		}
	}
	mvccStore.CurrentRevision = kv.store.currentRevision

	data, err := proto.Marshal(mvccStore)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// CleanupWALFiles 清理已快照的WAL文件 - 使用快照覆盖点逻辑
func (kv *KV) CleanupWALFiles(snapshotIndex uint64) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 更新最后快照索引
	kv.lastSnapshotIndex = int(snapshotIndex)

	return kv.walManager.CleanupWALFiles(snapshotIndex)
}

// GetWithTTL 获取键值对及其TTL信息
func (kv *KV) GetWithTTL(key string) (string, int64, error) {
	versionedKV, err := kv.store.Get(key, 0)
	if err != nil {
		return "", 0, err
	}
	return versionedKV.Value, versionedKV.TTL, nil
}

// 新增MVCC相关方法
func (kv *KV) GetWithRevision(key string, revision int64) (string, int64, error) {
	versionedKV, err := kv.store.Get(key, revision)
	if err != nil {
		return "", 0, err
	}
	return versionedKV.Value, versionedKV.ModRev, nil
}

func (kv *KV) GetHistory(key string, limit int64) ([]*VersionedKV, error) {
	return kv.store.GetHistory(key, limit)
}

func (kv *KV) Range(start, end string, revision int64, limit int64) ([]*VersionedKV, int64, error) {
	return kv.store.Range(start, end, revision, limit)
}

func (kv *KV) Compact(revision int64) (int64, error) {
	return kv.store.Compact(revision)
}

// ApplyTxn 在raft层应用事务操作
func (kv *KV) ApplyTxn(txnData string) error {
	// 解码事务数据
	data, err := base64.StdEncoding.DecodeString(txnData)
	if err != nil {
		return fmt.Errorf("failed to decode txn data: %v", err)
	}

	var req kvpb.TxnRequest
	if err := proto.Unmarshal(data, &req); err != nil {
		return fmt.Errorf("failed to unmarshal txn request: %v", err)
	}

	// 在raft层执行事务并写入WAL
	_, err = kv.Txn(&req)
	return err
}

// TxnWithoutWAL 事务操作，但不写入WAL（用于raft层应用）
func (kv *KV) TxnWithoutWAL(req *kvpb.TxnRequest) (*kvpb.TxnResponse, error) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	succeeded := true
	// 条件判断
	for _, cmp := range req.Compare {
		if !kv.evalCompare(cmp) {
			succeeded = false
			break
		}
	}

	var ops []*kvpb.Op
	if succeeded {
		ops = req.Success
	} else {
		ops = req.Failure
	}

	// 不写入WAL，直接执行操作
	responses := make([]*kvpb.OpResponse, 0, len(ops))
	for _, op := range ops {
		resp := kv.applyOp(op)
		responses = append(responses, resp)
	}

	return &kvpb.TxnResponse{
		Succeeded: succeeded,
		Responses: responses,
	}, nil
}

// Txn 事务操作，支持条件判断和原子提交
func (kv *KV) Txn(req *kvpb.TxnRequest) (*kvpb.TxnResponse, error) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	succeeded := true
	// 条件判断
	for _, cmp := range req.Compare {
		if !kv.evalCompare(cmp) {
			succeeded = false
			break
		}
	}

	var ops []*kvpb.Op
	if succeeded {
		ops = req.Success
	} else {
		ops = req.Failure
	}

	// 依次执行操作
	responses := make([]*kvpb.OpResponse, 0, len(ops))
	for _, op := range ops {
		resp := kv.applyOp(op)
		responses = append(responses, resp)
	}

	return &kvpb.TxnResponse{
		Succeeded: succeeded,
		Responses: responses,
	}, nil
}

// evalCompare 评估比较条件
func (kv *KV) evalCompare(cmp *kvpb.Compare) bool {
	versionedKV, err := kv.store.Get(cmp.Key, 0)
	if err != nil {
		// 键不存在
		if cmp.Target == kvpb.CompareTarget_EXISTS {
			return cmp.Result == kvpb.CompareResult_NOT_EQUAL
		}
		return false
	}

	switch cmp.Target {
	case kvpb.CompareTarget_VERSION:
		return kv.compareInt64(versionedKV.Version, cmp.Version, cmp.Result)
	case kvpb.CompareTarget_CREATE:
		return kv.compareInt64(versionedKV.CreatedRev, cmp.Version, cmp.Result)
	case kvpb.CompareTarget_MOD:
		return kv.compareInt64(versionedKV.ModRev, cmp.Version, cmp.Result)
	case kvpb.CompareTarget_VALUE:
		return kv.compareString(versionedKV.Value, cmp.Value, cmp.Result)
	case kvpb.CompareTarget_EXISTS:
		return cmp.Result == kvpb.CompareResult_EQUAL
	default:
		return false
	}
}

// compareInt64 比较整数
func (kv *KV) compareInt64(a, b int64, result kvpb.CompareResult) bool {
	switch result {
	case kvpb.CompareResult_EQUAL:
		return a == b
	case kvpb.CompareResult_GREATER:
		return a > b
	case kvpb.CompareResult_LESS:
		return a < b
	case kvpb.CompareResult_NOT_EQUAL:
		return a != b
	case kvpb.CompareResult_GREATER_EQUAL:
		return a >= b
	case kvpb.CompareResult_LESS_EQUAL:
		return a <= b
	default:
		return false
	}
}

// compareString 比较字符串
func (kv *KV) compareString(a, b string, result kvpb.CompareResult) bool {
	switch result {
	case kvpb.CompareResult_EQUAL:
		return a == b
	case kvpb.CompareResult_GREATER:
		return a > b
	case kvpb.CompareResult_LESS:
		return a < b
	case kvpb.CompareResult_NOT_EQUAL:
		return a != b
	case kvpb.CompareResult_GREATER_EQUAL:
		return a >= b
	case kvpb.CompareResult_LESS_EQUAL:
		return a <= b
	default:
		return false
	}
}

// applyOp 应用操作
func (kv *KV) applyOp(op *kvpb.Op) *kvpb.OpResponse {
	switch op.Type {
	case "PUT":
		revision, err := kv.store.Put(op.Key, string(op.Value), op.Ttl)
		if err != nil {
			return &kvpb.OpResponse{Error: err.Error()}
		}
		// 通知Watch监听器
		kv.watcher.NotifyPut(op.Key, string(op.Value), revision, op.Ttl)
		return &kvpb.OpResponse{Key: op.Key, Version: revision}

	case "GET":
		versionedKV, err := kv.store.Get(op.Key, 0)
		if err != nil {
			return &kvpb.OpResponse{Key: op.Key, Error: err.Error()}
		}
		return &kvpb.OpResponse{
			Key:     op.Key,
			Value:   versionedKV.Value,
			Version: versionedKV.ModRev,
			Exists:  true,
		}

	case "DELETE":
		revision, err := kv.store.Delete(op.Key)
		if err != nil {
			return &kvpb.OpResponse{Key: op.Key, Error: err.Error()}
		}
		// 通知Watch监听器
		kv.watcher.NotifyDelete(op.Key, revision)
		return &kvpb.OpResponse{Key: op.Key, Version: revision}

	default:
		return &kvpb.OpResponse{Error: "unknown operation type"}
	}
}

// GetStats 获取统计信息
func (kv *KV) GetStats() map[string]interface{} {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	stats := kv.store.GetStats()

	return stats
}

// Watch相关方法

// WatchKey 监听指定键的变化
func (kv *KV) WatchKey(key string) (*watch.Watcher, error) {
	return kv.watcher.WatchKey(key)
}

// WatchPrefix 监听指定前缀的所有键的变化
func (kv *KV) WatchPrefix(prefix string) (*watch.Watcher, error) {
	return kv.watcher.WatchPrefix(prefix)
}

// WatchKeyWithID 使用指定ID监听键的变化
func (kv *KV) WatchKeyWithID(id, key string) (*watch.Watcher, error) {
	return kv.watcher.WatchKeyWithID(id, key)
}

// WatchPrefixWithID 使用指定ID监听前缀的变化
func (kv *KV) WatchPrefixWithID(id, prefix string) (*watch.Watcher, error) {
	return kv.watcher.WatchPrefixWithID(id, prefix)
}

// Unwatch 取消监听
func (kv *KV) Unwatch(id string) error {
	return kv.watcher.Unwatch(id)
}

// GetWatcher 获取指定ID的监听器
func (kv *KV) GetWatcher(id string) (*watch.Watcher, bool) {
	return kv.watcher.GetWatcher(id)
}

// ListWatchers 列出所有监听器
func (kv *KV) ListWatchers() []*watch.Watcher {
	return kv.watcher.ListWatchers()
}

// GetWatchStats 获取监听器统计信息
func (kv *KV) GetWatchStats() map[string]interface{} {
	return kv.watcher.GetStats()
}

// CleanupWatchers 清理已关闭的监听器
func (kv *KV) CleanupWatchers() {
	kv.watcher.Cleanup()
}

// 新增：应用WALEntry到store
func (kv *KV) ApplyWALEntry(entry *kvpb.WALEntry) error {
	// 假设Data为protobuf序列化的kvpb.Op
	var op kvpb.Op
	if err := proto.Unmarshal(entry.Data, &op); err != nil {
		fmt.Printf("[WAL] Unmarshal Op failed: Index=%d, err=%v\n", entry.Index, err)
		return err
	}
	// 这里不再加锁，由ReplayWALsFrom统一加锁
	// 其余分支不再打印[WAL]日志
	switch op.Type {
	case "Put":
		_, err := kv.store.Put(op.Key, string(op.Value), 0)
		return err
	case "PutTTL":
		_, err := kv.store.Put(op.Key, string(op.Value), op.Ttl)
		return err
	case "Del":
		_, err := kv.store.Delete(op.Key)
		return err
	case "Txn":
		var req kvpb.TxnRequest
		if err := proto.Unmarshal(op.Value, &req); err != nil {
			fmt.Printf("[WAL] Unmarshal TxnRequest failed: Index=%d, err=%v\n", entry.Index, err)
			return err
		}
		succeeded := op.Ttl == 1 // 约定Ttl==1表示成功分支，否则失败分支
		var ops []*kvpb.Op
		if succeeded {
			ops = req.Success
		} else {
			ops = req.Failure
		}
		for _, subOp := range ops {
			kv.applyOp(subOp)
		}
		return nil
	case "Compact":
		revisionStr := string(op.Value)
		revision, err := strconv.ParseInt(revisionStr, 10, 64)
		if err != nil {
			return err
		}
		_, err = kv.store.Compact(revision)
		return err
	default:
		return fmt.Errorf("unknown op type: %s", op.Type)
	}
}

// 新增：重放WAL分段
func (kv *KV) ReplayWALsFrom(fromIdx uint64) error {
	return kv.walManager.ReplayAllWALs(kv.ApplyWALEntry, fromIdx)
}

func (kv *KV) GetWALManager() *WALManager {
	return kv.walManager
}

// 新增：直接应用TxnRequest对象
func (kv *KV) ApplyTxnProto(req *kvpb.TxnRequest) error {
	_, err := kv.Txn(req)
	return err
}
