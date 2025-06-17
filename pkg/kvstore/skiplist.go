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
}

type node struct {
	key   string
	value string
	next  []*node
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
func (sl *SkipList) Set(key, value string) {
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
		key:   key,
		value: value,
		next:  make([]*node, lvl),
	}
	for i := 0; i < lvl; i++ {
		n.next[i] = update[i].next[i]
		update[i].next[i] = n
	}
}

// Get 查找key
func (sl *SkipList) Get(key string) (string, bool) {
	x := sl.header
	for i := sl.level - 1; i >= 0; i-- {
		for x.next[i] != nil && x.next[i].key < key {
			x = x.next[i]
		}
	}
	x = x.next[0]
	if x != nil && x.key == key {
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

// Range 返回区间 [start, end) 内所有键值对，end为空串时表示无上界
func (sl *SkipList) Range(start, end string) []KVPair {
	var res []KVPair
	x := sl.header

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
		res = append(res, KVPair{Key: x.key, Value: x.value})
		x = x.next[0]
	}
	return res
}
