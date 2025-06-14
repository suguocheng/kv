package kvstore

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
)

// type KVStore interface {
// 	NewKV(filepath string) (*KV, error)
// 	Put(key string, value string) error
// 	Get(key string) (string, error)
// 	Delete(key string) error
// }

type KV struct {
	logFile *os.File
	writer  *bufio.Writer
	store   map[string]string
	mu      sync.RWMutex
}

func NewKV(filepath string) (*KV, error) {
	file, err := os.OpenFile(filepath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	kv := &KV{
		store:   make(map[string]string),
		logFile: file,
		writer:  bufio.NewWriter(file),
	}

	error := kv.replayLog()
	if error != nil {
		return nil, err
	}

	return kv, nil
}

func (kv *KV) replayLog() error {
	kv.logFile.Seek(0, 0) // 回到文件头
	scanner := bufio.NewScanner(kv.logFile)

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
			kv.store[parts[1]] = parts[2]
		case "DEL":
			delete(kv.store, parts[1])
		}
	}

	return scanner.Err()
}

func (kv *KV) Put(key, value string) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if _, exists := kv.store[key]; exists {
		return errors.New("key already exists")
	}

	logEntry := fmt.Sprintf("PUT %s %s\n", key, value)
	_, err := kv.writer.WriteString(logEntry)
	if err != nil {
		return err
	}
	kv.writer.Flush()

	kv.store[key] = value
	return nil
}

func (kv *KV) Get(key string) (string, error) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	val, ok := kv.store[key]
	if !ok {
		return "", errors.New("key does not exist")
	}

	return val, nil
}

func (kv *KV) Delete(key string) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if _, exists := kv.store[key]; !exists {
		return errors.New("key does not exist")
	}

	logEntry := fmt.Sprintf("DEL %s\n", key)
	_, err := kv.writer.WriteString(logEntry)
	if err != nil {
		return err
	}
	kv.writer.Flush()

	delete(kv.store, key)
	return nil
}

func (kv *KV) Close() error {
	kv.writer.Flush()
	return kv.logFile.Close()
}

func (kv *KV) SerializeState() []byte {
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	encoder.Encode(kv.store)
	return buffer.Bytes()
}
