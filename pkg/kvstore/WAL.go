package kvstore

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"kv/pkg/proto/kvpb"

	"google.golang.org/protobuf/proto"
)

type EntryType int

const (
	EntryNormal EntryType = iota
	EntryConfChange
	EntryMeta
)

type WALFile struct {
	file       *os.File
	path       string
	entries    int
	maxEntries int
	StartIdx   uint64
	EndIdx     uint64
}

type WALManager struct {
	walDir     string
	walFiles   []*WALFile
	currentWAL *WALFile
	maxEntries int
}

func NewWALManager(walDir string, maxEntriesPerFile int) (*WALManager, error) {
	if err := os.MkdirAll(walDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create WAL directory: %v", err)
	}
	wm := &WALManager{
		walDir:     walDir,
		maxEntries: maxEntriesPerFile,
		walFiles:   make([]*WALFile, 0),
	}

	// 恢复所有WAL分段
	files, err := os.ReadDir(walDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read WAL directory: %v", err)
	}
	var walFileNames []string
	for _, f := range files {
		if !f.IsDir() && strings.HasSuffix(f.Name(), ".wal") {
			walFileNames = append(walFileNames, f.Name())
		}
	}
	sort.Strings(walFileNames)
	for _, fname := range walFileNames {
		walPath := filepath.Join(walDir, fname)
		file, err := os.OpenFile(walPath, os.O_RDWR|os.O_APPEND, 0644)
		if err != nil {
			return nil, fmt.Errorf("failed to open WAL file: %v", err)
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
			maxEntries: maxEntriesPerFile,
			StartIdx:   startIdx,
			EndIdx:     actualEndIdx,
		}
		wm.walFiles = append(wm.walFiles, walFile)
		wm.currentWAL = walFile // 最后一个分段作为currentWAL
	}
	return wm, nil
}

func (wm *WALManager) createNewWALFile(startIdx uint64) error {
	endIdx := startIdx + uint64(wm.maxEntries) - 1
	walPath := filepath.Join(wm.walDir, fmt.Sprintf("%d-%d.wal", startIdx, endIdx))
	file, err := os.OpenFile(walPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	walFile := &WALFile{
		file:       file,
		path:       walPath,
		entries:    0,
		maxEntries: wm.maxEntries,
		StartIdx:   startIdx,
		EndIdx:     startIdx - 1, // 实际写入的最大Index
	}
	wm.walFiles = append(wm.walFiles, walFile)
	wm.currentWAL = walFile
	return nil
}

func (wm *WALManager) WriteEntry(entry *kvpb.WALEntry) error {
	if wm.currentWAL == nil || wm.currentWAL.entries >= wm.currentWAL.maxEntries {
		var newStartIdx uint64 = entry.Index
		if wm.currentWAL != nil {
			newStartIdx = wm.currentWAL.StartIdx + uint64(wm.currentWAL.maxEntries)
		}
		if err := wm.createNewWALFile(newStartIdx); err != nil {
			return err
		}
	}
	data, err := proto.Marshal(entry)
	if err != nil {
		return err
	}
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(data)))
	if _, err := wm.currentWAL.file.Write(lenBuf[:]); err != nil {
		return err
	}
	if _, err := wm.currentWAL.file.Write(data); err != nil {
		return err
	}
	wm.currentWAL.entries++
	wm.currentWAL.EndIdx = entry.Index
	return wm.currentWAL.file.Sync()
}

func (wm *WALManager) ReplayAllWALs(apply func(*kvpb.WALEntry) error, fromIdx uint64) error {
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

func (wm *WALManager) CleanupWALFiles(snapshotIdx uint64) error {
	var filesToDelete []string
	var filesToKeep []*WALFile
	for _, walFile := range wm.walFiles {
		if walFile.EndIdx > 0 && walFile.EndIdx <= snapshotIdx {
			filesToDelete = append(filesToDelete, walFile.path)
		} else {
			filesToKeep = append(filesToKeep, walFile)
		}
	}
	for _, filePath := range filesToDelete {
		os.Remove(filePath)
	}
	wm.walFiles = filesToKeep
	if len(wm.walFiles) == 0 {
		wm.currentWAL = nil
		_ = wm.createNewWALFile(snapshotIdx + 1)
	} else {
		wm.currentWAL = wm.walFiles[len(wm.walFiles)-1]
	}
	return nil
}
