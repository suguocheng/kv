package watch

import (
	"testing"
	"time"
)

func TestWatchManager(t *testing.T) {
	wm := NewWatchManager()

	// 测试创建监听器
	watcher, err := wm.Watch("test_id", "test_key", "")
	if err != nil {
		t.Errorf("Watch failed: %v", err)
	}

	if watcher.ID != "test_id" {
		t.Errorf("Expected ID test_id, got %s", watcher.ID)
	}

	if watcher.Key != "test_key" {
		t.Errorf("Expected Key test_key, got %s", watcher.Key)
	}

	// 测试重复ID
	_, err = wm.Watch("test_id", "another_key", "")
	if err == nil {
		t.Error("Expected error for duplicate ID")
	}
}

func TestWatchManagerNotify(t *testing.T) {
	wm := NewWatchManager()

	// 创建监听器
	watcher, _ := wm.Watch("test_id", "test_key", "")

	// 创建事件
	event := &Event{
		Type:      EventPut,
		Key:       "test_key",
		Value:     "test_value",
		Revision:  1,
		Timestamp: time.Now(),
	}

	// 通知事件
	wm.Notify(event)

	// 检查事件是否被接收
	select {
	case receivedEvent := <-watcher.Events:
		if receivedEvent.Key != event.Key {
			t.Errorf("Expected key %s, got %s", event.Key, receivedEvent.Key)
		}
		if receivedEvent.Value != event.Value {
			t.Errorf("Expected value %s, got %s", event.Value, receivedEvent.Value)
		}
	case <-time.After(1 * time.Second):
		t.Error("Timeout waiting for event")
	}
}

func TestWatchManagerUnwatch(t *testing.T) {
	wm := NewWatchManager()

	// 创建监听器
	watcher, _ := wm.Watch("test_id", "test_key", "")

	// 取消监听
	err := wm.Unwatch("test_id")
	if err != nil {
		t.Errorf("Unwatch failed: %v", err)
	}

	// 验证监听器已关闭
	if !watcher.IsClosed() {
		t.Error("Expected watcher to be closed")
	}

	// 测试取消不存在的监听器
	err = wm.Unwatch("nonexistent_id")
	if err == nil {
		t.Error("Expected error for nonexistent watcher")
	}
}

func TestWatchManagerMultipleWatchers(t *testing.T) {
	wm := NewWatchManager()

	// 创建多个监听器监听同一个键
	watcher1, _ := wm.Watch("id1", "test_key", "")
	watcher2, _ := wm.Watch("id2", "test_key", "")

	// 创建事件
	event := &Event{
		Type:      EventPut,
		Key:       "test_key",
		Value:     "test_value",
		Revision:  1,
		Timestamp: time.Now(),
	}

	// 通知事件
	wm.Notify(event)

	// 检查两个监听器都收到事件
	select {
	case receivedEvent := <-watcher1.Events:
		if receivedEvent.Key != event.Key {
			t.Errorf("Watcher1: Expected key %s, got %s", event.Key, receivedEvent.Key)
		}
	case <-time.After(1 * time.Second):
		t.Error("Timeout waiting for watcher1 event")
	}

	select {
	case receivedEvent := <-watcher2.Events:
		if receivedEvent.Key != event.Key {
			t.Errorf("Watcher2: Expected key %s, got %s", event.Key, receivedEvent.Key)
		}
	case <-time.After(1 * time.Second):
		t.Error("Timeout waiting for watcher2 event")
	}
}

func TestWatchManagerEventTypes(t *testing.T) {
	wm := NewWatchManager()

	// 创建监听器
	watcher, _ := wm.Watch("test_id", "test_key", "")

	// 测试PUT事件
	putEvent := &Event{
		Type:      EventPut,
		Key:       "test_key",
		Value:     "test_value",
		Revision:  1,
		Timestamp: time.Now(),
	}

	wm.Notify(putEvent)

	select {
	case receivedEvent := <-watcher.Events:
		if receivedEvent.Type != EventPut {
			t.Errorf("Expected PUT event, got %v", receivedEvent.Type)
		}
	case <-time.After(1 * time.Second):
		t.Error("Timeout waiting for PUT event")
	}

	// 测试DELETE事件
	deleteEvent := &Event{
		Type:      EventDelete,
		Key:       "test_key",
		Revision:  2,
		Timestamp: time.Now(),
	}

	wm.Notify(deleteEvent)

	select {
	case receivedEvent := <-watcher.Events:
		if receivedEvent.Type != EventDelete {
			t.Errorf("Expected DELETE event, got %v", receivedEvent.Type)
		}
	case <-time.After(1 * time.Second):
		t.Error("Timeout waiting for DELETE event")
	}
}

func TestWatchManagerStats(t *testing.T) {
	wm := NewWatchManager()

	// 创建一些监听器
	wm.Watch("id1", "key1", "")
	wm.Watch("id2", "key2", "")

	stats := wm.GetStats()

	// 验证统计信息
	if stats["total_watchers"] != 2 {
		t.Errorf("Expected 2 total watchers, got %v", stats["total_watchers"])
	}

	if stats["key_watchers"] != 2 {
		t.Errorf("Expected 2 key watchers, got %v", stats["key_watchers"])
	}

	if stats["active_watchers"] != 2 {
		t.Errorf("Expected 2 active watchers, got %v", stats["active_watchers"])
	}
}

func TestWatchManagerCleanup(t *testing.T) {
	wm := NewWatchManager()

	// 创建监听器
	watcher, _ := wm.Watch("test_id", "test_key", "")

	// 手动关闭监听器
	watcher.Close()

	// 清理
	wm.Cleanup()

	// 验证监听器被清理
	stats := wm.GetStats()
	if stats["total_watchers"] != 0 {
		t.Errorf("Expected 0 total watchers after cleanup, got %v", stats["total_watchers"])
	}
}

func TestEventString(t *testing.T) {
	event := &Event{
		Type:      EventPut,
		Key:       "test_key",
		Value:     "test_value",
		Revision:  1,
		Timestamp: time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC),
		TTL:       0,
	}

	str := event.String()
	expected := "Event{Type: PUT, Key: test_key, Value: test_value, Revision: 1, Timestamp: 2023-01-01T12:00:00Z, TTL: 0}"

	if str != expected {
		t.Errorf("Expected %s, got %s", expected, str)
	}
}

func TestEventTypeString(t *testing.T) {
	testCases := []struct {
		eventType EventType
		expected  string
	}{
		{EventPut, "PUT"},
		{EventDelete, "DELETE"},
		{EventExpire, "EXPIRE"},
		{EventType(99), "UNKNOWN"},
	}

	for _, tc := range testCases {
		result := tc.eventType.String()
		if result != tc.expected {
			t.Errorf("Expected %s for EventType %d, got %s", tc.expected, tc.eventType, result)
		}
	}
}
