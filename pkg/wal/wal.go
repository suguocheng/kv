package wal

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"kv/pkg/proto/kvpb"

	"google.golang.org/protobuf/proto"
)

// WALFile WAL文件信息
type WALFile struct {
	file       *os.File
	path       string
	entries    int
	maxEntries int
	StartIdx   uint64
	EndIdx     uint64
}

// WALEntry 异步WAL条目，包含写入完成通知
type WALEntry struct {
	Entry     *kvpb.WALEntry
	Done      chan error
	WriteTime time.Time
}

// WALManager 异步WAL管理器
type WALManager struct {
	mu         sync.RWMutex
	walDir     string
	walFiles   []*WALFile
	currentWAL *WALFile
	maxEntries int

	// 异步写入相关字段
	writeBuffer   []byte
	bufferSize    int
	flushInterval time.Duration
	flushChan     chan struct{}
	stopChan      chan struct{}
	writeChan     chan *WALEntry
	workerWg      sync.WaitGroup

	// 统计信息
	stats struct {
		sync.Mutex
		TotalWrites  int64
		TotalLatency time.Duration
		FlushCount   int64
		BufferHits   int64
		BufferMisses int64
	}
}

// NewWALManager 创建异步WAL管理器
func NewWALManager(walDir string, maxEntriesPerFile int) (*WALManager, error) {
	if err := os.MkdirAll(walDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create WAL directory: %v", err)
	}

	wm := &WALManager{
		walDir:        walDir,
		maxEntries:    maxEntriesPerFile,
		walFiles:      make([]*WALFile, 0),
		writeBuffer:   make([]byte, 0, 64*1024), // 64KB初始缓冲区
		bufferSize:    64 * 1024,                // 64KB缓冲区大小
		flushInterval: 100 * time.Millisecond,   // 100ms刷盘间隔
		flushChan:     make(chan struct{}, 1),   // 非阻塞刷盘信号
		stopChan:      make(chan struct{}),
		writeChan:     make(chan *WALEntry, 1000), // 1000个条目的写入队列
	}

	// 恢复现有WAL文件
	if err := wm.recoverWALFiles(); err != nil {
		return nil, err
	}

	// 启动异步写入工作协程
	wm.startAsyncWorker()

	return wm, nil
}

// recoverWALFiles 恢复现有WAL文件
func (wm *WALManager) recoverWALFiles() error {
	files, err := os.ReadDir(wm.walDir)
	if err != nil {
		return fmt.Errorf("failed to read WAL directory: %v", err)
	}

	var walFileNames []string
	for _, f := range files {
		if !f.IsDir() && strings.HasSuffix(f.Name(), ".wal") {
			walFileNames = append(walFileNames, f.Name())
		}
	}
	sort.Strings(walFileNames)

	for _, fname := range walFileNames {
		walPath := filepath.Join(wm.walDir, fname)
		file, err := os.OpenFile(walPath, os.O_RDWR|os.O_APPEND, 0644)
		if err != nil {
			return fmt.Errorf("failed to open WAL file: %v", err)
		}

		// 解析文件名获取startIdx
		base := strings.TrimSuffix(fname, ".wal")
		parts := strings.Split(base, "-")
		if len(parts) != 2 {
			file.Close()
			continue
		}

		startIdx, _ := strconv.ParseUint(parts[0], 10, 64)

		// 统计实际entries和EndIdx
		entries := 0
		actualEndIdx := startIdx - 1
		file.Seek(0, 0)

		for {
			var lenBuf [4]byte
			_, err := file.Read(lenBuf[:])
			if err != nil {
				break
			}

			msgLen := binary.BigEndian.Uint32(lenBuf[:])
			msgBuf := make([]byte, msgLen)
			_, err = file.Read(msgBuf)
			if err != nil {
				break
			}

			var entry kvpb.WALEntry
			if err := proto.Unmarshal(msgBuf, &entry); err != nil {
				break
			}

			entries++
			actualEndIdx = entry.Index
		}

		walFile := &WALFile{
			file:       file,
			path:       walPath,
			entries:    entries,
			maxEntries: wm.maxEntries,
			StartIdx:   startIdx,
			EndIdx:     actualEndIdx,
		}

		wm.walFiles = append(wm.walFiles, walFile)
	}

	// 设置当前WAL文件
	if len(wm.walFiles) > 0 {
		wm.currentWAL = wm.walFiles[len(wm.walFiles)-1]
	}

	return nil
}

// startAsyncWorker 启动异步写入工作协程
func (wm *WALManager) startAsyncWorker() {
	wm.workerWg.Add(1)
	go func() {
		defer wm.workerWg.Done()
		wm.asyncWorker()
	}()
}

// asyncWorker 异步写入工作协程
func (wm *WALManager) asyncWorker() {
	ticker := time.NewTicker(wm.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case entry := <-wm.writeChan:
			// 处理写入请求
			wm.processWriteEntry(entry)

		case <-wm.flushChan:
			// 手动刷盘
			wm.flushBuffer()

		case <-ticker.C:
			// 定期刷盘
			wm.flushBuffer()

		case <-wm.stopChan:
			// 停止信号
			wm.flushBuffer() // 最后刷盘
			return
		}
	}
}

// processWriteEntry 处理写入条目
func (wm *WALManager) processWriteEntry(asyncEntry *WALEntry) {
	start := time.Now()

	// 序列化条目
	data, err := proto.Marshal(asyncEntry.Entry)
	if err != nil {
		asyncEntry.Done <- err
		return
	}

	// 添加到缓冲区
	wm.addToBuffer(data)

	// 更新WAL文件EndIdx
	wm.mu.Lock()
	if wm.currentWAL != nil && asyncEntry.Entry.Index > wm.currentWAL.EndIdx {
		wm.currentWAL.EndIdx = asyncEntry.Entry.Index
	}
	wm.mu.Unlock()

	// 更新统计信息
	wm.stats.Lock()
	wm.stats.TotalWrites++
	wm.stats.TotalLatency += time.Since(start)
	wm.stats.Unlock()

	// 检查是否需要刷盘
	if len(wm.writeBuffer) >= wm.bufferSize {
		wm.flushBuffer()
		wm.stats.Lock()
		wm.stats.BufferHits++
		wm.stats.Unlock()
	} else {
		wm.stats.Lock()
		wm.stats.BufferMisses++
		wm.stats.Unlock()
	}

	// 通知写入完成
	asyncEntry.Done <- nil
}

// addToBuffer 添加数据到缓冲区
func (wm *WALManager) addToBuffer(data []byte) {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	// 检查缓冲区容量
	if cap(wm.writeBuffer) < len(wm.writeBuffer)+len(data)+4 {
		// 扩容缓冲区
		newSize := cap(wm.writeBuffer) * 2
		if newSize < len(wm.writeBuffer)+len(data)+4 {
			newSize = len(wm.writeBuffer) + len(data) + 4
		}
		newBuffer := make([]byte, len(wm.writeBuffer), newSize)
		copy(newBuffer, wm.writeBuffer)
		wm.writeBuffer = newBuffer
	}

	// 添加长度前缀
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(data)))
	wm.writeBuffer = append(wm.writeBuffer, lenBuf[:]...)

	// 添加数据
	wm.writeBuffer = append(wm.writeBuffer, data...)
}

// flushBuffer 刷盘缓冲区
func (wm *WALManager) flushBuffer() {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	if len(wm.writeBuffer) == 0 {
		return
	}

	// 确保当前WAL文件存在
	if wm.currentWAL == nil {
		// 创建第一个WAL文件
		if err := wm.createNewWALFile(1); err != nil {
			return
		}
	}

	// 计算写入的条目数（每个条目有4字节长度前缀）
	entriesInBuffer := 0
	offset := 0
	for offset < len(wm.writeBuffer) {
		if offset+4 > len(wm.writeBuffer) {
			break
		}
		msgLen := binary.BigEndian.Uint32(wm.writeBuffer[offset : offset+4])
		offset += 4 + int(msgLen)
		entriesInBuffer++
	}

	// 写入缓冲区数据
	if _, err := wm.currentWAL.file.Write(wm.writeBuffer); err != nil {
		return
	}

	// 同步到磁盘
	if err := wm.currentWAL.file.Sync(); err != nil {
		return
	}

	// 更新WAL文件统计信息
	wm.currentWAL.entries += entriesInBuffer
	if wm.currentWAL.entries >= wm.currentWAL.maxEntries {
		// 当前文件已满，创建新文件
		newStartIdx := wm.currentWAL.EndIdx + 1
		if err := wm.createNewWALFile(newStartIdx); err != nil {
			return
		}
	}

	// 清空缓冲区
	wm.writeBuffer = wm.writeBuffer[:0]

	// 更新统计信息
	wm.stats.Lock()
	wm.stats.FlushCount++
	wm.stats.Unlock()
}

// WriteEntry 异步写入条目
func (wm *WALManager) WriteEntry(entry *kvpb.WALEntry) error {
	if entry == nil {
		return fmt.Errorf("entry cannot be nil")
	}

	asyncEntry := &WALEntry{
		Entry:     entry,
		Done:      make(chan error, 1),
		WriteTime: time.Now(),
	}

	// 发送到写入队列
	select {
	case wm.writeChan <- asyncEntry:
		// 等待写入完成
		return <-asyncEntry.Done
	case <-time.After(5 * time.Second):
		return fmt.Errorf("write timeout")
	}
}

// WriteEntrySync 同步写入条目（用于关键操作）
func (wm *WALManager) WriteEntrySync(entry *kvpb.WALEntry) error {
	// 先异步写入
	if err := wm.WriteEntry(entry); err != nil {
		return err
	}

	// 强制刷盘并等待完成
	select {
	case wm.flushChan <- struct{}{}:
		// 等待刷盘完成
		time.Sleep(50 * time.Millisecond)
	default:
		// 如果刷盘信号队列满了，直接等待
		time.Sleep(50 * time.Millisecond)
	}

	return nil
}

// createNewWALFile 创建新的WAL文件
func (wm *WALManager) createNewWALFile(startIdx uint64) error {
	filename := fmt.Sprintf("%d-%d.wal", startIdx, startIdx+uint64(wm.maxEntries)-1)
	walPath := filepath.Join(wm.walDir, filename)

	file, err := os.OpenFile(walPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to create WAL file: %v", err)
	}

	walFile := &WALFile{
		file:       file,
		path:       walPath,
		entries:    0,
		maxEntries: wm.maxEntries,
		StartIdx:   startIdx,
		EndIdx:     startIdx - 1,
	}

	wm.walFiles = append(wm.walFiles, walFile)
	wm.currentWAL = walFile

	return nil
}

// Close 关闭异步WAL管理器
func (wm *WALManager) Close() error {
	// 防止重复关闭
	select {
	case <-wm.stopChan:
		// 已经关闭
		return nil
	default:
		// 发送停止信号
		close(wm.stopChan)
	}

	// 等待工作协程结束
	wm.workerWg.Wait()

	// 关闭所有文件
	for _, walFile := range wm.walFiles {
		if walFile.file != nil {
			walFile.file.Close()
		}
	}

	return nil
}

// GetStats 获取统计信息
func (wm *WALManager) GetStats() map[string]interface{} {
	wm.stats.Lock()
	defer wm.stats.Unlock()

	var avgLatency time.Duration
	if wm.stats.TotalWrites > 0 {
		avgLatency = wm.stats.TotalLatency / time.Duration(wm.stats.TotalWrites)
	}

	return map[string]interface{}{
		"total_writes":    wm.stats.TotalWrites,
		"total_latency":   wm.stats.TotalLatency,
		"avg_latency":     avgLatency,
		"flush_count":     wm.stats.FlushCount,
		"buffer_hits":     wm.stats.BufferHits,
		"buffer_misses":   wm.stats.BufferMisses,
		"buffer_size":     len(wm.writeBuffer),
		"buffer_capacity": cap(wm.writeBuffer),
		"wal_files_count": len(wm.walFiles),
	}
}

// ReplayAllWALs 重放所有WAL文件（保持与原有接口兼容）
func (wm *WALManager) ReplayAllWALs(apply func(*kvpb.WALEntry) error, fromIdx uint64) error {
	wm.mu.RLock()
	defer wm.mu.RUnlock()

	files, err := os.ReadDir(wm.walDir)
	if err != nil {
		return err
	}

	var walFiles []string
	for _, f := range files {
		if !f.IsDir() && strings.HasSuffix(f.Name(), ".wal") {
			walFiles = append(walFiles, filepath.Join(wm.walDir, f.Name()))
		}
	}
	sort.Strings(walFiles)

	var anyErr error
	for _, walPath := range walFiles {
		file, err := os.Open(walPath)
		if err != nil {
			fmt.Printf("[WAL] 打开WAL文件失败: %s, err=%v\n", walPath, err)
			anyErr = err
			continue
		}

		for {
			var lenBuf [4]byte
			_, err := file.Read(lenBuf[:])
			if err != nil {
				break
			}

			msgLen := binary.BigEndian.Uint32(lenBuf[:])
			msgBuf := make([]byte, msgLen)
			_, err = file.Read(msgBuf)
			if err != nil {
				break
			}

			var entry kvpb.WALEntry
			if err := proto.Unmarshal(msgBuf, &entry); err != nil {
				fmt.Printf("[WAL] ReplayAllWALs: proto.Unmarshal failed: %v\n", err)
				anyErr = err
				continue
			}

			if entry.Index >= fromIdx {
				if err := apply(&entry); err != nil {
					fmt.Printf("[WAL] ReplayAllWALs: apply entry Index=%d failed: %v\n", entry.Index, err)
					anyErr = err
					continue
				}
			}
		}
		file.Close()
	}

	return anyErr
}

// CleanupWALFiles 清理WAL文件（保持与原有接口兼容）
func (wm *WALManager) CleanupWALFiles(snapshotIdx uint64) error {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	var filesToDelete []string
	var newWALFiles []*WALFile

	for _, walFile := range wm.walFiles {
		if walFile.EndIdx <= snapshotIdx {
			filesToDelete = append(filesToDelete, walFile.path)
			if walFile.file != nil {
				walFile.file.Close()
			}
		} else {
			newWALFiles = append(newWALFiles, walFile)
		}
	}

	// 删除文件
	for _, filePath := range filesToDelete {
		if err := os.Remove(filePath); err != nil {
			// 如果文件不存在，忽略错误
			if !os.IsNotExist(err) {
				return fmt.Errorf("failed to remove WAL file %s: %v", filePath, err)
			}
		}
	}

	wm.walFiles = newWALFiles
	return nil
}
