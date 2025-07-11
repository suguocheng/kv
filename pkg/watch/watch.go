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
	Type      EventType `json:"type"`
	Key       string    `json:"key"`
	Value     string    `json:"value,omitempty"`
	Revision  int64     `json:"revision"`
	Timestamp time.Time `json:"timestamp"`
	TTL       int64     `json:"ttl,omitempty"`
}

// String 返回事件的字符串表示
func (e *Event) String() string {
	return fmt.Sprintf("Event{Type: %s, Key: %s, Value: %s, Revision: %d, Timestamp: %s, TTL: %d}",
		e.Type, e.Key, e.Value, e.Revision, e.Timestamp.Format(time.RFC3339), e.TTL)
}

// WatchResponse 表示监听响应
type WatchResponse struct {
	Events []*Event `json:"events"`
	Error  error    `json:"error,omitempty"`
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
	watchers  map[string]*Watcher // 按ID索引
	keyMap    map[string][]string // 按key索引watcher ID列表
	prefixMap map[string][]string // 按prefix索引watcher ID列表
	mu        sync.RWMutex
}

// NewWatchManager 创建一个新的监听管理器
func NewWatchManager() *WatchManager {
	return &WatchManager{
		watchers:  make(map[string]*Watcher),
		keyMap:    make(map[string][]string),
		prefixMap: make(map[string][]string),
	}
}

// Watch 创建一个新的监听器
func (wm *WatchManager) Watch(id, key, prefix string) (*Watcher, error) {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	// 检查ID是否已存在
	if _, exists := wm.watchers[id]; exists {
		return nil, fmt.Errorf("watcher with ID %s already exists", id)
	}

	// 创建新的监听器
	watcher := NewWatcher(id, key, prefix)
	wm.watchers[id] = watcher

	// 注册到keyMap
	if key != "" {
		wm.keyMap[key] = append(wm.keyMap[key], id)
	}

	// 注册到prefixMap
	if prefix != "" {
		wm.prefixMap[prefix] = append(wm.prefixMap[prefix], id)
	}

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
		wm.removeFromSlice(wm.keyMap[watcher.Key], id)
		if len(wm.keyMap[watcher.Key]) == 0 {
			delete(wm.keyMap, watcher.Key)
		}
	}

	// 从prefixMap中删除
	if watcher.Prefix != "" {
		wm.removeFromSlice(wm.prefixMap[watcher.Prefix], id)
		if len(wm.prefixMap[watcher.Prefix]) == 0 {
			delete(wm.prefixMap, watcher.Prefix)
		}
	}

	return nil
}

// Notify 通知所有相关的监听器
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

	// 通知前缀匹配的监听器
	for prefix, watcherIDs := range wm.prefixMap {
		if wm.hasPrefix(event.Key, prefix) {
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
	stats["prefix_watchers"] = len(wm.prefixMap)

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
			wm.removeFromSlice(wm.keyMap[watcher.Key], id)
			if len(wm.keyMap[watcher.Key]) == 0 {
				delete(wm.keyMap, watcher.Key)
			}
		}

		// 从prefixMap中删除
		if watcher.Prefix != "" {
			wm.removeFromSlice(wm.prefixMap[watcher.Prefix], id)
			if len(wm.prefixMap[watcher.Prefix]) == 0 {
				delete(wm.prefixMap, watcher.Prefix)
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

// hasPrefix 检查字符串是否有指定前缀
func (wm *WatchManager) hasPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}
