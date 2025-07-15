package kvstore

import (
	"bufio"
	"encoding/base64"
	"fmt"
	"kv/pkg/kvpb"
	"kv/pkg/watch"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
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

	// 1. 先从快照恢复，并设置lastSnapshotIndex
	_ = kv.RestoreFromSnapshot()
	// 读取快照点index
	data, err := os.ReadFile(snapshotPath)
	fromIdx := uint64(1)
	if err == nil && len(data) > 0 {
		var kvStore kvpb.KVStore
		if proto.Unmarshal(data, &kvStore) == nil {
			if len(kvStore.Pairs) > 0 {
				// 取最大Ttl作为快照点（如无ModRev字段，可用其他合适字段，或直接跳过，fromIdx=1）
				maxIdx := int64(0)
				for _, pair := range kvStore.Pairs {
					if pair.Ttl > maxIdx {
						maxIdx = pair.Ttl
					}
				}
				kv.lastSnapshotIndex = int(maxIdx)
				fromIdx = uint64(maxIdx + 1)
			}
		}
	}
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

		kv.store.Put(pair.Key, pair.Value, ttl)
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

		kv.store.Put(pair.Key, pair.Value, ttl)
		restored++
	}

	if restored > 0 || skipped > 0 {
		fmt.Printf("Snapshot data restore: restored %d keys, skipped %d potentially expired keys\n", restored, skipped)
	}

	// 覆盖本地快照文件
	os.WriteFile(kv.snapshotPath, snapshot, 0644)
	return nil
}

// Put/PutWithTTL/Delete等方法只做业务apply，不再写WAL
func (kv *KV) Put(key, value string) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, err := kv.store.Put(key, value, 0)
	return err
}

func (kv *KV) PutWithTTL(key, value string, ttl int64) error {
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

	// WAL关闭由WALManager内部管理，不在此处调用
	// return kv.walManager.Close()
	return nil
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
func (kv *KV) CleanupWALFiles(snapshotIndex uint64) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 更新最后快照索引
	kv.lastSnapshotIndex = int(snapshotIndex)

	return kv.walManager.CleanupWALFiles(snapshotIndex)
}

// GetWALStats 获取WAL统计信息
// func (kv *KV) GetWALStats() map[string]interface{} {
// 	kv.mu.RLock()
// 	defer kv.mu.RUnlock()
// 	return kv.walManager.GetWALStats()
// }

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

// replayWALs 重放WAL文件到MVCC存储
func (kv *KV) replayWALs() error {
	files, err := os.ReadDir(kv.walManager.walDir)
	if err != nil {
		return fmt.Errorf("failed to read WAL directory: %v", err)
	}

	var walFiles []string
	for _, file := range files {
		if !file.IsDir() && strings.HasPrefix(file.Name(), "wal_") && strings.HasSuffix(file.Name(), ".log") {
			walFiles = append(walFiles, filepath.Join(kv.walManager.walDir, file.Name()))
		}
	}

	sort.Strings(walFiles)
	for _, walPath := range walFiles {
		if err := kv.replayWALFile(walPath); err != nil {
			return err
		}
	}

	return nil
}

// replayWALFile 重放单个WAL文件
func (kv *KV) replayWALFile(walPath string) error {
	file, err := os.OpenFile(walPath, os.O_RDONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, " ", 5)
		if len(parts) < 2 {
			continue
		}

		kv.LastAppliedIndex++ // 每重放一条，递增lastAppliedIndex

		switch parts[0] {
		case "PUT":
			if len(parts) >= 4 {
				ttl, _ := strconv.ParseInt(parts[3], 10, 64)
				kv.store.Put(parts[1], parts[2], ttl)
			} else if len(parts) == 3 {
				kv.store.Put(parts[1], parts[2], 0)
			}
		case "DELETE":
			kv.store.Delete(parts[1])
		case "TXN":
			// 处理事务操作
			if len(parts) >= 4 {
				succeeded := parts[1] == "true"
				txnData := parts[3]
				// 重新构造事务请求并执行
				data, err := base64.StdEncoding.DecodeString(txnData)
				if err != nil {
					continue
				}
				var req kvpb.TxnRequest
				if err := proto.Unmarshal(data, &req); err != nil {
					continue
				}
				// 根据原始的成功/失败状态执行操作
				var ops []*kvpb.Op
				if succeeded {
					ops = req.Success
				} else {
					ops = req.Failure
				}
				// 直接执行操作，不写入WAL（因为这是重放）
				for _, op := range ops {
					kv.applyOp(op)
				}
			}
		}
	}

	return scanner.Err()
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

	// 如果有写操作，需要写入WAL（已由raft层负责，这里不再写WAL）
	// if len(ops) > 0 && kv.hasWriteOps(ops) {
	// 	logEntry := kv.formatTxnToWAL(req, succeeded)
	// 	if err := kv.walManager.WriteEntry(logEntry); err != nil {
	// 		return nil, fmt.Errorf("failed to write txn to WAL: %v", err)
	// 	}
	// }

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

// hasWriteOps 检查操作列表中是否包含写操作
func (kv *KV) hasWriteOps(ops []*kvpb.Op) bool {
	for _, op := range ops {
		if op.Type == "PUT" || op.Type == "DELETE" || op.Type == "DEL" {
			return true
		}
	}
	return false
}

// formatTxnToWAL 将事务格式化为WAL日志字符串
func (kv *KV) formatTxnToWAL(req *kvpb.TxnRequest, succeeded bool) string {
	timestamp := time.Now().Unix()

	// 格式: TXN <succeeded> <timestamp> <base64_encoded_request>
	txnData, _ := proto.Marshal(req)
	b64Data := base64.StdEncoding.EncodeToString(txnData)

	return fmt.Sprintf("TXN %t %d %s", succeeded, timestamp, b64Data)
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
	// walStats := kv.walManager.GetWALStats()

	// 合并统计信息
	// for k, v := range walStats {
	// 	stats[k] = v
	// }

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
func (kv *KV) ApplyWALEntry(entry *WALEntry) error {
	// 假设Data为protobuf序列化的kvpb.Op
	var op kvpb.Op
	if err := proto.Unmarshal(entry.Data, &op); err != nil {
		return err
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
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
		// 反序列化TxnRequest
		var req kvpb.TxnRequest
		if err := proto.Unmarshal(op.Value, &req); err != nil {
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
		revision, err := strconv.ParseInt(string(op.Value), 10, 64)
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
