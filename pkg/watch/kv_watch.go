package watch

import (
	"fmt"
	"sync"
	"time"
)

// KVWatcher 提供与KV存储集成的监听功能
type KVWatcher struct {
	watchManager *WatchManager
	mu           sync.RWMutex
}

// NewKVWatcher 创建一个新的KV监听器
func NewKVWatcher() *KVWatcher {
	return &KVWatcher{
		watchManager: NewWatchManager(),
	}
}

// WatchKey 监听指定键的变化
func (kw *KVWatcher) WatchKey(key string) (*Watcher, error) {
	id := fmt.Sprintf("key_%s_%d", key, time.Now().UnixNano())
	return kw.watchManager.Watch(id, key, "")
}

// WatchPrefix 监听指定前缀的所有键的变化
func (kw *KVWatcher) WatchPrefix(prefix string) (*Watcher, error) {
	id := fmt.Sprintf("prefix_%s_%d", prefix, time.Now().UnixNano())
	return kw.watchManager.Watch(id, "", prefix)
}

// WatchKeyWithID 使用指定ID监听键的变化
func (kw *KVWatcher) WatchKeyWithID(id, key string) (*Watcher, error) {
	return kw.watchManager.Watch(id, key, "")
}

// WatchPrefixWithID 使用指定ID监听前缀的变化
func (kw *KVWatcher) WatchPrefixWithID(id, prefix string) (*Watcher, error) {
	return kw.watchManager.Watch(id, "", prefix)
}

// Unwatch 取消监听
func (kw *KVWatcher) Unwatch(id string) error {
	return kw.watchManager.Unwatch(id)
}

// GetWatcher 获取指定ID的监听器
func (kw *KVWatcher) GetWatcher(id string) (*Watcher, bool) {
	return kw.watchManager.GetWatcher(id)
}

// ListWatchers 列出所有监听器
func (kw *KVWatcher) ListWatchers() []*Watcher {
	return kw.watchManager.ListWatchers()
}

// GetStats 获取监听器统计信息
func (kw *KVWatcher) GetStats() map[string]interface{} {
	return kw.watchManager.GetStats()
}

// Cleanup 清理已关闭的监听器
func (kw *KVWatcher) Cleanup() {
	kw.watchManager.Cleanup()
}

// NotifyPut 通知PUT事件
func (kw *KVWatcher) NotifyPut(key, value string, revision, ttl int64) {
	event := &Event{
		Type:      EventPut,
		Key:       key,
		Value:     value,
		Revision:  revision,
		Timestamp: time.Now(),
		TTL:       ttl,
	}
	kw.watchManager.Notify(event)
}

// NotifyDelete 通知DELETE事件
func (kw *KVWatcher) NotifyDelete(key string, revision int64) {
	event := &Event{
		Type:      EventDelete,
		Key:       key,
		Revision:  revision,
		Timestamp: time.Now(),
	}
	kw.watchManager.Notify(event)
}

// NotifyExpire 通知EXPIRE事件
func (kw *KVWatcher) NotifyExpire(key string, revision int64) {
	event := &Event{
		Type:      EventExpire,
		Key:       key,
		Revision:  revision,
		Timestamp: time.Now(),
	}
	kw.watchManager.Notify(event)
}

// WatchStream 提供流式监听接口
type WatchStream struct {
	watcher *Watcher
	events  chan *Event
	errors  chan error
	done    chan struct{}
}

// NewWatchStream 创建一个新的监听流
func NewWatchStream(watcher *Watcher) *WatchStream {
	stream := &WatchStream{
		watcher: watcher,
		events:  make(chan *Event, 100),
		errors:  make(chan error, 10),
		done:    make(chan struct{}),
	}

	// 启动事件转发goroutine
	go stream.forwardEvents()

	return stream
}

// forwardEvents 转发事件到流
func (ws *WatchStream) forwardEvents() {
	defer close(ws.events)
	defer close(ws.errors)
	defer close(ws.done)

	for {
		select {
		case event, ok := <-ws.watcher.Events:
			if !ok {
				return
			}
			select {
			case ws.events <- event:
			case <-ws.done:
				return
			}
		case <-ws.watcher.ctx.Done():
			ws.errors <- fmt.Errorf("watcher context cancelled")
			return
		case <-ws.done:
			return
		}
	}
}

// Events 返回事件通道
func (ws *WatchStream) Events() <-chan *Event {
	return ws.events
}

// Errors 返回错误通道
func (ws *WatchStream) Errors() <-chan error {
	return ws.errors
}

// Close 关闭监听流
func (ws *WatchStream) Close() {
	close(ws.done)
	ws.watcher.Close()
}

// Done 返回done通道
func (ws *WatchStream) Done() <-chan struct{} {
	return ws.done
}

// WatchOptions 监听选项
type WatchOptions struct {
	StartRevision int64         // 开始监听的版本号
	EndRevision   int64         // 结束监听的版本号（0表示不限制）
	Timeout       time.Duration // 超时时间
	BufferSize    int           // 缓冲区大小
}

// DefaultWatchOptions 默认监听选项
func DefaultWatchOptions() *WatchOptions {
	return &WatchOptions{
		StartRevision: 0,
		EndRevision:   0,
		Timeout:       0, // 无超时
		BufferSize:    100,
	}
}

// WatchWithOptions 使用选项创建监听器
func (kw *KVWatcher) WatchWithOptions(key, prefix string, options *WatchOptions) (*Watcher, error) {
	if options == nil {
		options = DefaultWatchOptions()
	}

	var id string
	if key != "" {
		id = fmt.Sprintf("key_%s_%d", key, time.Now().UnixNano())
	} else if prefix != "" {
		id = fmt.Sprintf("prefix_%s_%d", prefix, time.Now().UnixNano())
	} else {
		return nil, fmt.Errorf("either key or prefix must be specified")
	}

	watcher, err := kw.watchManager.Watch(id, key, prefix)
	if err != nil {
		return nil, err
	}

	// 如果指定了超时，设置定时器
	if options.Timeout > 0 {
		go func() {
			time.Sleep(options.Timeout)
			watcher.Close()
		}()
	}

	return watcher, nil
}
