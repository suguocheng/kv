package watch

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// EventType 表示事件类型
type EventType int

const (
	EventPut EventType = iota
	EventDelete
	EventExpire
)

// String 返回事件类型的字符串表示
func (e EventType) String() string {
	switch e {
	case EventPut:
		return "PUT"
	case EventDelete:
		return "DELETE"
	case EventExpire:
		return "EXPIRE"
	default:
		return "UNKNOWN"
	}
}

// Event 表示一个键值变化事件
type Event struct {
	Type      EventType
	Key       string
	Value     string
	Revision  int64
	Timestamp time.Time
	TTL       int64
}

// String 返回事件的字符串表示
func (e *Event) String() string {
	return fmt.Sprintf("Event{Type: %s, Key: %s, Value: %s, Revision: %d, Timestamp: %s, TTL: %d}",
		e.Type, e.Key, e.Value, e.Revision, e.Timestamp.Format(time.RFC3339), e.TTL)
}

// WatchResponse 表示监听响应
type WatchResponse struct {
	Events []*Event
	Error  error
}

// Watcher 表示一个监听器
type Watcher struct {
	ID       string
	Key      string
	Prefix   string
	Events   chan *Event
	Cancel   context.CancelFunc
	ctx      context.Context
	mu       sync.RWMutex
	isClosed bool
}

// NewWatcher 创建一个新的监听器
func NewWatcher(id, key, prefix string) *Watcher {
	ctx, cancel := context.WithCancel(context.Background())
	return &Watcher{
		ID:     id,
		Key:    key,
		Prefix: prefix,
		Events: make(chan *Event, 100), // 缓冲通道，避免阻塞
		Cancel: cancel,
		ctx:    ctx,
	}
}

// Close 关闭监听器
func (w *Watcher) Close() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.isClosed {
		w.isClosed = true
		w.Cancel()
		close(w.Events)
	}
}

// IsClosed 检查监听器是否已关闭
func (w *Watcher) IsClosed() bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.isClosed
}

// WatchManager 管理所有的监听器
type WatchManager struct {
	watchers map[string]*Watcher // 按ID索引
	keyMap   map[string][]string // 按key索引watcher ID列表
	mu       sync.RWMutex
}

// NewWatchManager 创建一个新的监听管理器
func NewWatchManager() *WatchManager {
	return &WatchManager{
		watchers: make(map[string]*Watcher),
		keyMap:   make(map[string][]string),
	}
}

// Watch 创建一个新的监听器（仅支持按Key）
func (wm *WatchManager) Watch(id, key, prefix string) (*Watcher, error) {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	// 检查ID是否已存在
	if _, exists := wm.watchers[id]; exists {
		return nil, fmt.Errorf("watcher with ID %s already exists", id)
	}

	// 仅支持按Key监听，忽略prefix
	if key == "" {
		return nil, fmt.Errorf("key must be specified for watch")
	}

	// 创建新的监听器
	watcher := NewWatcher(id, key, "")
	wm.watchers[id] = watcher

	// 注册到keyMap
	wm.keyMap[key] = append(wm.keyMap[key], id)

	return watcher, nil
}

// Unwatch 取消监听
func (wm *WatchManager) Unwatch(id string) error {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	watcher, exists := wm.watchers[id]
	if !exists {
		return fmt.Errorf("watcher with ID %s not found", id)
	}

	// 关闭监听器
	watcher.Close()

	// 从watchers中删除
	delete(wm.watchers, id)

	// 从keyMap中删除
	if watcher.Key != "" {
		wm.keyMap[watcher.Key] = wm.removeFromSlice(wm.keyMap[watcher.Key], id)
		if len(wm.keyMap[watcher.Key]) == 0 {
			delete(wm.keyMap, watcher.Key)
		}
	}

	return nil
}

// Notify 通知所有相关的监听器（仅按Key）
func (wm *WatchManager) Notify(event *Event) {
	wm.mu.RLock()
	defer wm.mu.RUnlock()

	// 通知精确匹配的key监听器
	if watcherIDs, exists := wm.keyMap[event.Key]; exists {
		for _, id := range watcherIDs {
			if watcher, ok := wm.watchers[id]; ok && !watcher.IsClosed() {
				select {
				case watcher.Events <- event:
				default:
					// 通道已满，跳过这个事件
				}
			}
		}
	}
}

// GetWatcher 获取指定ID的监听器
func (wm *WatchManager) GetWatcher(id string) (*Watcher, bool) {
	wm.mu.RLock()
	defer wm.mu.RUnlock()
	watcher, exists := wm.watchers[id]
	return watcher, exists
}

// ListWatchers 列出所有监听器
func (wm *WatchManager) ListWatchers() []*Watcher {
	wm.mu.RLock()
	defer wm.mu.RUnlock()

	watchers := make([]*Watcher, 0, len(wm.watchers))
	for _, watcher := range wm.watchers {
		watchers = append(watchers, watcher)
	}
	return watchers
}

// GetStats 获取监听器统计信息
func (wm *WatchManager) GetStats() map[string]interface{} {
	wm.mu.RLock()
	defer wm.mu.RUnlock()

	stats := make(map[string]interface{})
	stats["total_watchers"] = len(wm.watchers)
	stats["key_watchers"] = len(wm.keyMap)

	// 统计活跃的监听器数量
	activeCount := 0
	for _, watcher := range wm.watchers {
		if !watcher.IsClosed() {
			activeCount++
		}
	}
	stats["active_watchers"] = activeCount

	return stats
}

// Cleanup 清理已关闭的监听器
func (wm *WatchManager) Cleanup() {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	// 收集需要删除的监听器ID
	var toDelete []string
	for id, watcher := range wm.watchers {
		if watcher.IsClosed() {
			toDelete = append(toDelete, id)
		}
	}

	// 删除已关闭的监听器
	for _, id := range toDelete {
		watcher := wm.watchers[id]
		delete(wm.watchers, id)

		// 从keyMap中删除
		if watcher.Key != "" {
			wm.keyMap[watcher.Key] = wm.removeFromSlice(wm.keyMap[watcher.Key], id)
			if len(wm.keyMap[watcher.Key]) == 0 {
				delete(wm.keyMap, watcher.Key)
			}
		}
	}
}

// removeFromSlice 从切片中删除指定元素
func (wm *WatchManager) removeFromSlice(slice []string, item string) []string {
	for i, v := range slice {
		if v == item {
			return append(slice[:i], slice[i+1:]...)
		}
	}
	return slice
}
