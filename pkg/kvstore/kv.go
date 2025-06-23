package kvstore

import (
	"bufio"
	"errors"
	"fmt"
	"kv/pkg/kvpb"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"google.golang.org/protobuf/proto"
)

// type KVStore interface {
// 	NewKV(filepath string) (*KV, error)
// 	Put(key string, value string) error
// 	Get(key string) (string, error)
// 	Delete(key string) error
// }

type WALFile struct {
	file       *os.File
	writer     *bufio.Writer
	path       string
	entries    int // 当前文件中的日志条目数量
	maxEntries int // 每个WAL文件最大条目数

	startIndex int // 新增：该WAL文件包含的第一个Raft日志索引
	endIndex   int // 新增：该WAL文件包含的最后一个Raft日志索引（闭区间）
}

type KV struct {
	walDir            string
	walFiles          []*WALFile
	currentWAL        *WALFile
	store             *SkipList
	mu                sync.RWMutex
	maxEntries        int    // 每个WAL文件最大条目数
	lastApplied       int    // 最后应用的日志索引
	snapshotPath      string // 快照文件路径
	walSeq            int    // 新增：下一个WAL文件编号
	lastSnapshotIndex int    // 新增：最后快照的索引

	nextRaftIndex int // 新增：下一个Raft日志索引
}

func NewKV(walDir string, maxEntriesPerFile int) (*KV, error) {
	if err := os.MkdirAll(walDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create WAL directory: %v", err)
	}
	// 构造快照文件路径
	snapshotPath := filepath.Join(filepath.Dir(walDir), "snapshot.pb")
	kv := &KV{
		walDir:        walDir,
		store:         NewSkipList(),
		maxEntries:    maxEntriesPerFile,
		walFiles:      make([]*WALFile, 0),
		snapshotPath:  snapshotPath,
		walSeq:        0,
		nextRaftIndex: 1, // 初始Raft日志索引
	}
	// 1. 先从快照恢复
	_ = kv.RestoreFromSnapshot()
	// 2. 再重放WAL
	_ = kv.replayAllWALs()
	// 3. 新建WAL文件，编号递增
	if err := kv.createNewWALFile(); err != nil {
		return nil, err
	}
	return kv, nil
}

// 新增：快照恢复
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

// 修正：新建WAL文件时编号递增
func (kv *KV) createNewWALFile() error {
	// 找到下一个WAL文件编号
	nextID := kv.walSeq
	walPath := filepath.Join(kv.walDir, fmt.Sprintf("wal_%06d.log", nextID))
	file, err := os.OpenFile(walPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to create WAL file %s: %v", walPath, err)
	}
	walFile := &WALFile{
		file:       file,
		writer:     bufio.NewWriter(file),
		path:       walPath,
		entries:    0,
		maxEntries: kv.maxEntries,
		startIndex: kv.nextRaftIndex,
		endIndex:   kv.nextRaftIndex - 1,
	}
	kv.walFiles = append(kv.walFiles, walFile)
	kv.currentWAL = walFile
	kv.walSeq++        // 递增编号
	kv.nextRaftIndex++ // 递增Raft日志索引
	return nil
}

// 修正：重放WAL时同步walSeq和nextRaftIndex
func (kv *KV) replayAllWALs() error {
	files, err := os.ReadDir(kv.walDir)
	if err != nil {
		return fmt.Errorf("failed to read WAL directory: %v", err)
	}
	var walFiles []string
	maxID := -1
	for _, file := range files {
		if !file.IsDir() && strings.HasPrefix(file.Name(), "wal_") && strings.HasSuffix(file.Name(), ".log") {
			walFiles = append(walFiles, filepath.Join(kv.walDir, file.Name()))
			id, _ := kv.extractWALID(file.Name())
			if id > maxID {
				maxID = id
			}
		}
	}
	sort.Strings(walFiles)
	for _, walPath := range walFiles {
		_ = kv.replayWALFile(walPath)
	}
	kv.walSeq = maxID + 1 // 保证新建文件编号递增
	// 修正：恢复nextRaftIndex
	if len(kv.walFiles) > 0 {
		last := kv.walFiles[len(kv.walFiles)-1]
		kv.nextRaftIndex = last.endIndex + 1
	} else {
		kv.nextRaftIndex = 1
	}
	return nil
}

// 重放单个WAL文件
func (kv *KV) replayWALFile(walPath string) error {
	file, err := os.OpenFile(walPath, os.O_RDONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	entries := 0
	startIndex := kv.nextRaftIndex
	endIndex := kv.nextRaftIndex - 1

	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, " ", 3)
		if len(parts) < 2 {
			continue
		}

		switch parts[0] {
		case "PUT":
			if len(parts) != 3 {
				continue
			}
			kv.store.Set(parts[1], parts[2])
		case "DEL":
			kv.store.Delete(parts[1])
		}
		entries++
		endIndex = kv.nextRaftIndex
		kv.nextRaftIndex++
	}

	// 将已重放的文件添加到WAL文件列表中（用于后续清理）
	walFile := &WALFile{
		file:       nil, // 已关闭
		writer:     nil,
		path:       walPath,
		entries:    entries,
		maxEntries: kv.maxEntries,
		startIndex: startIndex,
		endIndex:   endIndex,
	}
	kv.walFiles = append(kv.walFiles, walFile)

	return scanner.Err()
}

func (kv *KV) extractWALID(path string) (int, error) {
	filename := filepath.Base(path)
	if !strings.HasPrefix(filename, "wal_") || !strings.HasSuffix(filename, ".log") {
		return 0, fmt.Errorf("invalid WAL filename format: %s", filename)
	}

	idStr := strings.TrimSuffix(strings.TrimPrefix(filename, "wal_"), ".log")
	return strconv.Atoi(idStr)
}

func (kv *KV) writeLogEntry(entry string) error {
	// 检查当前WAL文件是否存在且未满
	if kv.currentWAL == nil || kv.currentWAL.entries >= kv.currentWAL.maxEntries {
		// 当前WAL文件不存在或已满，创建新文件
		if err := kv.createNewWALFile(); err != nil {
			return err
		}
	}

	_, err := kv.currentWAL.writer.WriteString(entry + "\n")
	if err != nil {
		return err
	}

	if err := kv.currentWAL.writer.Flush(); err != nil {
		return err
	}

	kv.currentWAL.entries++
	// 新增：每次写入时，更新endIndex为当前Raft日志索引
	kv.currentWAL.endIndex = kv.nextRaftIndex
	kv.nextRaftIndex++
	return nil
}

func (kv *KV) Put(key, value string) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if _, exists := kv.store.Get(key); exists {
		return errors.New("key already exists")
	}

	logEntry := fmt.Sprintf("PUT %s %s", key, value)
	if err := kv.writeLogEntry(logEntry); err != nil {
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
	if err := kv.writeLogEntry(logEntry); err != nil {
		return err
	}

	kv.store.Delete(key)
	return nil
}

func (kv *KV) Close() error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 刷新当前WAL文件
	if kv.currentWAL != nil && kv.currentWAL.writer != nil {
		kv.currentWAL.writer.Flush()
		kv.currentWAL.file.Close()
	}

	return nil
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

	fmt.Printf("DEBUG: CleanupWALFiles - snapshotIndex=%d\n", snapshotIndex)

	// 计算每个WAL文件的覆盖范围
	var filesToDelete []string
	var filesToKeep []*WALFile

	for _, walFile := range kv.walFiles {
		fmt.Printf("DEBUG: WAL file %s - startIndex=%d, endIndex=%d\n", filepath.Base(walFile.path), walFile.startIndex, walFile.endIndex)
		// 只有endIndex<=snapshotIndex时才删除
		if walFile.endIndex > 0 && walFile.endIndex <= snapshotIndex {
			filesToDelete = append(filesToDelete, walFile.path)
			fmt.Printf("DEBUG: Will delete WAL file %s\n", filepath.Base(walFile.path))
		} else {
			filesToKeep = append(filesToKeep, walFile)
			fmt.Printf("DEBUG: Will keep WAL file %s\n", filepath.Base(walFile.path))
		}
	}

	// 删除已快照的WAL文件
	for _, filePath := range filesToDelete {
		if err := os.Remove(filePath); err != nil {
			return fmt.Errorf("failed to delete WAL file %s: %v", filePath, err)
		}
	}

	// 更新WAL文件列表，只保留未删除的文件
	kv.walFiles = filesToKeep

	// 重要：检查当前WAL文件是否被删除，如果是则设置为nil
	// 这样下次写入时会自动创建新文件
	if kv.currentWAL != nil {
		currentWALExists := false
		for _, walFile := range kv.walFiles {
			if walFile.path == kv.currentWAL.path {
				currentWALExists = true
				break
			}
		}
		if !currentWALExists {
			fmt.Printf("DEBUG: Current WAL file %s was deleted, setting to nil\n", filepath.Base(kv.currentWAL.path))
			kv.currentWAL = nil
		}
	}

	// 如果所有WAL文件都被删光了，主动新建一个WAL文件
	if len(kv.walFiles) == 0 {
		fmt.Printf("DEBUG: All WAL files deleted, creating new WAL file\n")
		if err := kv.createNewWALFile(); err != nil {
			return fmt.Errorf("failed to create new WAL file after cleanup: %v", err)
		}
	}

	return nil
}

// GetWALStats 获取WAL统计信息
func (kv *KV) GetWALStats() map[string]interface{} {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	totalEntries := 0
	for _, walFile := range kv.walFiles {
		totalEntries += walFile.entries
	}

	currentWALEntries := 0
	if kv.currentWAL != nil {
		currentWALEntries = kv.currentWAL.entries
	}

	return map[string]interface{}{
		"total_wal_files":      len(kv.walFiles),
		"total_entries":        totalEntries,
		"max_entries_per_file": kv.maxEntries,
		"current_wal_entries":  currentWALEntries,
	}
}
