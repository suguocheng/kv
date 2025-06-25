package kvstore

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// WALFile 表示一个WAL文件
type WALFile struct {
	file       *os.File
	writer     *bufio.Writer
	path       string
	entries    int // 当前文件中的日志条目数量
	maxEntries int // 每个WAL文件最大条目数

	startIndex int // 该WAL文件包含的第一个Raft日志索引
	endIndex   int // 该WAL文件包含的最后一个Raft日志索引（闭区间）
}

// WALManager 管理WAL文件
type WALManager struct {
	walDir            string
	walFiles          []*WALFile
	currentWAL        *WALFile
	maxEntries        int // 每个WAL文件最大条目数
	walSeq            int // 下一个WAL文件编号
	nextRaftIndex     int // 下一个Raft日志索引
	lastSnapshotIndex int // 最后快照的索引
}

// NewWALManager 创建新的WAL管理器
func NewWALManager(walDir string, maxEntriesPerFile int) (*WALManager, error) {
	if err := os.MkdirAll(walDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create WAL directory: %v", err)
	}

	return &WALManager{
		walDir:        walDir,
		maxEntries:    maxEntriesPerFile,
		walFiles:      make([]*WALFile, 0),
		walSeq:        0,
		nextRaftIndex: 1, // 初始Raft日志索引
	}, nil
}

// ReplayAllWALs 重放所有WAL文件
func (wm *WALManager) ReplayAllWALs(store *SkipList) error {
	files, err := os.ReadDir(wm.walDir)
	if err != nil {
		return fmt.Errorf("failed to read WAL directory: %v", err)
	}

	var walFiles []string
	maxID := -1
	for _, file := range files {
		if !file.IsDir() && strings.HasPrefix(file.Name(), "wal_") && strings.HasSuffix(file.Name(), ".log") {
			walFiles = append(walFiles, filepath.Join(wm.walDir, file.Name()))
			id, _ := wm.extractWALID(file.Name())
			if id > maxID {
				maxID = id
			}
		}
	}

	sort.Strings(walFiles)
	for _, walPath := range walFiles {
		_ = wm.replayWALFile(walPath, store)
	}

	wm.walSeq = maxID + 1 // 保证新建文件编号递增
	// 修正：恢复nextRaftIndex
	if len(wm.walFiles) > 0 {
		last := wm.walFiles[len(wm.walFiles)-1]
		wm.nextRaftIndex = last.endIndex + 1
	} else {
		wm.nextRaftIndex = 1
	}

	// 检查最新的WAL文件是否还有空间，如果有就继续使用
	if len(wm.walFiles) > 0 {
		latestWAL := wm.walFiles[len(wm.walFiles)-1]
		if latestWAL.entries < latestWAL.maxEntries {
			// 最新WAL文件还有空间，重新打开进行追加写入
			if err := wm.reopenLatestWALFile(latestWAL); err != nil {
				return fmt.Errorf("failed to reopen latest WAL file: %v", err)
			}
		} else {
			// 最新WAL文件已满，需要创建新文件
			if err := wm.createNewWALFile(); err != nil {
				return fmt.Errorf("failed to create new WAL file: %v", err)
			}
		}
	} else {
		// 没有现有WAL文件，创建新文件
		if err := wm.createNewWALFile(); err != nil {
			return fmt.Errorf("failed to create new WAL file: %v", err)
		}
	}

	return nil
}

// replayWALFile 重放单个WAL文件
func (wm *WALManager) replayWALFile(walPath string, store *SkipList) error {
	file, err := os.OpenFile(walPath, os.O_RDONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	entries := 0
	startIndex := wm.nextRaftIndex
	endIndex := wm.nextRaftIndex - 1

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
			store.Set(parts[1], parts[2])
		case "DEL":
			store.Delete(parts[1])
		}
		entries++
		endIndex = wm.nextRaftIndex
		wm.nextRaftIndex++
	}

	// 将已重放的文件添加到WAL文件列表中（用于后续清理）
	walFile := &WALFile{
		file:       nil, // 已关闭
		writer:     nil,
		path:       walPath,
		entries:    entries,
		maxEntries: wm.maxEntries,
		startIndex: startIndex,
		endIndex:   endIndex,
	}
	wm.walFiles = append(wm.walFiles, walFile)

	return scanner.Err()
}

// extractWALID 从WAL文件名中提取ID
func (wm *WALManager) extractWALID(path string) (int, error) {
	filename := filepath.Base(path)
	if !strings.HasPrefix(filename, "wal_") || !strings.HasSuffix(filename, ".log") {
		return 0, fmt.Errorf("invalid WAL filename format: %s", filename)
	}

	idStr := strings.TrimSuffix(strings.TrimPrefix(filename, "wal_"), ".log")
	return strconv.Atoi(idStr)
}

// reopenLatestWALFile 重新打开最新的WAL文件进行追加写入
func (wm *WALManager) reopenLatestWALFile(walFile *WALFile) error {
	file, err := os.OpenFile(walFile.path, os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to reopen WAL file %s: %v", walFile.path, err)
	}

	// 更新WALFile结构，重新设置file和writer
	walFile.file = file
	walFile.writer = bufio.NewWriter(file)

	// 设置为当前WAL文件
	wm.currentWAL = walFile

	return nil
}

// createNewWALFile 创建新的WAL文件
func (wm *WALManager) createNewWALFile() error {
	// 找到下一个WAL文件编号
	nextID := wm.walSeq
	walPath := filepath.Join(wm.walDir, fmt.Sprintf("wal_%06d.log", nextID))
	file, err := os.OpenFile(walPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to create WAL file %s: %v", walPath, err)
	}

	walFile := &WALFile{
		file:       file,
		writer:     bufio.NewWriter(file),
		path:       walPath,
		entries:    0,
		maxEntries: wm.maxEntries,
		startIndex: wm.nextRaftIndex,
		endIndex:   wm.nextRaftIndex - 1,
	}

	wm.walFiles = append(wm.walFiles, walFile)
	wm.currentWAL = walFile
	wm.walSeq++        // 递增编号
	wm.nextRaftIndex++ // 递增Raft日志索引
	return nil
}

// WriteLogEntry 写入日志条目
func (wm *WALManager) WriteLogEntry(entry string) error {
	// 检查当前WAL文件是否存在且未满
	if wm.currentWAL == nil || wm.currentWAL.entries >= wm.currentWAL.maxEntries {
		// 当前WAL文件不存在或已满，创建新文件
		if err := wm.createNewWALFile(); err != nil {
			return err
		}
	}

	_, err := wm.currentWAL.writer.WriteString(entry + "\n")
	if err != nil {
		return err
	}

	if err := wm.currentWAL.writer.Flush(); err != nil {
		return err
	}

	wm.currentWAL.entries++
	// 每次写入时，更新endIndex为当前Raft日志索引
	wm.currentWAL.endIndex = wm.nextRaftIndex
	wm.nextRaftIndex++
	return nil
}

// Close 关闭WAL管理器
func (wm *WALManager) Close() error {
	// 刷新当前WAL文件
	if wm.currentWAL != nil && wm.currentWAL.writer != nil {
		wm.currentWAL.writer.Flush()
		wm.currentWAL.file.Close()
	}
	return nil
}

// CleanupWALFiles 清理已快照的WAL文件
func (wm *WALManager) CleanupWALFiles(snapshotIndex int) error {
	// 更新最后快照索引
	wm.lastSnapshotIndex = snapshotIndex

	// 计算每个WAL文件的覆盖范围
	var filesToDelete []string
	var filesToKeep []*WALFile

	for _, walFile := range wm.walFiles {
		// 只有endIndex<=snapshotIndex时才删除
		if walFile.endIndex > 0 && walFile.endIndex <= snapshotIndex {
			filesToDelete = append(filesToDelete, walFile.path)
		} else {
			filesToKeep = append(filesToKeep, walFile)
		}
	}

	// 删除已快照的WAL文件
	for _, filePath := range filesToDelete {
		if err := os.Remove(filePath); err != nil {
			return fmt.Errorf("failed to delete WAL file %s: %v", filePath, err)
		}
	}

	// 更新WAL文件列表，只保留未删除的文件
	wm.walFiles = filesToKeep

	// 重要：检查当前WAL文件是否被删除，如果是则设置为nil
	// 这样下次写入时会自动创建新文件
	if wm.currentWAL != nil {
		currentWALExists := false
		for _, walFile := range wm.walFiles {
			if walFile.path == wm.currentWAL.path {
				currentWALExists = true
				break
			}
		}
		if !currentWALExists {
			wm.currentWAL = nil
		}
	}

	// 如果所有WAL文件都被删光了，主动新建一个WAL文件
	if len(wm.walFiles) == 0 {
		if err := wm.createNewWALFile(); err != nil {
			return fmt.Errorf("failed to create new WAL file after cleanup: %v", err)
		}
	}

	return nil
}

// GetWALStats 获取WAL统计信息
func (wm *WALManager) GetWALStats() map[string]interface{} {
	totalEntries := 0
	for _, walFile := range wm.walFiles {
		totalEntries += walFile.entries
	}

	currentWALEntries := 0
	if wm.currentWAL != nil {
		currentWALEntries = wm.currentWAL.entries
	}

	return map[string]interface{}{
		"total_wal_files":      len(wm.walFiles),
		"total_entries":        totalEntries,
		"max_entries_per_file": wm.maxEntries,
		"current_wal_entries":  currentWALEntries,
	}
}
