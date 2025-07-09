package kvstore

import (
	"math/rand"
	"time"
)

const (
	maxLevel    = 16
	probability = 0.25
)

type KVPair struct {
	Key   string
	Value string
	TTL   int64 // TTL in seconds, 0 means no expiration
}

type node struct {
	key       string
	value     string
	ttl       int64 // TTL in seconds, 0 means no expiration
	createdAt int64 // Unix timestamp when the key was created
	next      []*node
}

type SkipList struct {
	header *node
	level  int
	rand   *rand.Rand
}

func NewSkipList() *SkipList {
	return &SkipList{
		header: &node{
			next: make([]*node, maxLevel),
		},
		level: 1,
		rand:  rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// randomLevel 生成节点层数
func (sl *SkipList) randomLevel() int {
	lvl := 1
	for lvl < maxLevel && sl.rand.Float64() < probability {
		lvl++
	}
	return lvl
}

// Set 插入或更新
func (sl *SkipList) Set(key, value string, ttl int64) {
	update := make([]*node, maxLevel)
	x := sl.header

	// 找到每层第一个大于等于key的节点
	for i := sl.level - 1; i >= 0; i-- {
		for x.next[i] != nil && x.next[i].key < key {
			x = x.next[i]
		}
		update[i] = x
	}

	x = x.next[0]
	if x != nil && x.key == key {
		// 更新
		x.value = value
		x.ttl = ttl
		x.createdAt = time.Now().Unix()
		return
	}

	// 插入新节点
	lvl := sl.randomLevel()
	if lvl > sl.level {
		for i := sl.level; i < lvl; i++ {
			update[i] = sl.header
		}
		sl.level = lvl
	}

	n := &node{
		key:       key,
		value:     value,
		ttl:       ttl,
		createdAt: time.Now().Unix(),
		next:      make([]*node, lvl),
	}
	for i := 0; i < lvl; i++ {
		n.next[i] = update[i].next[i]
		update[i].next[i] = n
	}
}

// Get 查找key，如果过期返回false
func (sl *SkipList) Get(key string) (string, bool) {
	x := sl.header
	for i := sl.level - 1; i >= 0; i-- {
		for x.next[i] != nil && x.next[i].key < key {
			x = x.next[i]
		}
	}
	x = x.next[0]
	if x != nil && x.key == key {
		// 检查是否过期
		if x.ttl > 0 {
			now := time.Now().Unix()
			if now-x.createdAt >= x.ttl {
				return "", false // 已过期
			}
		}
		return x.value, true
	}
	return "", false
}

// Delete 删除key，成功返回true
func (sl *SkipList) Delete(key string) bool {
	update := make([]*node, maxLevel)
	x := sl.header
	for i := sl.level - 1; i >= 0; i-- {
		for x.next[i] != nil && x.next[i].key < key {
			x = x.next[i]
		}
		update[i] = x
	}

	x = x.next[0]
	if x == nil || x.key != key {
		return false
	}

	for i := 0; i < sl.level; i++ {
		if update[i].next[i] != x {
			break
		}
		update[i].next[i] = x.next[i]
	}

	// 降低层数
	for sl.level > 1 && sl.header.next[sl.level-1] == nil {
		sl.level--
	}
	return true
}

// Range 返回区间 [start, end) 内所有键值对，end为空串时表示无上界，自动过滤过期键
func (sl *SkipList) Range(start, end string) []KVPair {
	var res []KVPair
	x := sl.header
	now := time.Now().Unix()

	// 定位到 >= start 的节点
	for i := sl.level - 1; i >= 0; i-- {
		for x.next[i] != nil && x.next[i].key < start {
			x = x.next[i]
		}
	}
	x = x.next[0]

	for x != nil {
		if end != "" && x.key >= end {
			break
		}
		// 检查是否过期
		if x.ttl > 0 && now-x.createdAt >= x.ttl {
			// 跳过过期键
			x = x.next[0]
			continue
		}
		res = append(res, KVPair{Key: x.key, Value: x.value, TTL: x.ttl})
		x = x.next[0]
	}
	return res
}

// CleanupExpired 清理过期的键值对，返回清理的数量
func (sl *SkipList) CleanupExpired() int {
	cleaned := 0
	now := time.Now().Unix()
	x := sl.header.next[0]

	for x != nil {
		next := x.next[0] // 保存下一个节点，因为当前节点可能被删除
		if x.ttl > 0 && now-x.createdAt >= x.ttl {
			sl.Delete(x.key)
			cleaned++
		}
		x = next
	}
	return cleaned
}

// GetAllWithTTL 获取所有键值对（包括TTL信息），用于快照
func (sl *SkipList) GetAllWithTTL() []KVPair {
	var res []KVPair
	x := sl.header.next[0]

	for x != nil {
		res = append(res, KVPair{Key: x.key, Value: x.value, TTL: x.ttl})
		x = x.next[0]
	}
	return res
}
