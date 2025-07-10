package kvstore

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
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
