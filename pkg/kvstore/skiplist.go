package kvstore

import (
	"math/rand"
	"sync"
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

// VersionedKV 表示带版本的键值对
// 字段含义参考etcd等实现
type VersionedKV struct {
	Key        string
	Value      string
	TTL        int64
	CreatedRev int64
	ModRev     int64
	Version    int64
	Deleted    bool
	CreatedAt  int64
}

// Node 带版本信息的跳表节点
type Node struct {
	key      string
	versions []*VersionedKV // 版本历史，按版本号排序
	next     []*Node
}

// SkipList 支持MVCC和TTL的跳表
type SkipList struct {
	header          *Node
	level           int
	rand            *rand.Rand
	mu              sync.RWMutex
	currentRevision int64
	compactedRev    int64
}

// NewSkipList 创建新的跳表
func NewSkipList() *SkipList {
	return &SkipList{
		header: &Node{
			next: make([]*Node, maxLevel),
		},
		level:           1,
		rand:            rand.New(rand.NewSource(time.Now().UnixNano())),
		currentRevision: 1,
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

// Put 插入或更新键值对，创建新版本
func (sl *SkipList) Put(key, value string, ttl int64) (int64, error) {
	sl.mu.Lock()
	defer sl.mu.Unlock()

	// 分配新版本号
	revision := sl.currentRevision
	sl.currentRevision++

	// 创建新版本
	versionedKV := &VersionedKV{
		Key:        key,
		Value:      value,
		TTL:        ttl,
		CreatedRev: revision,
		ModRev:     revision,
		Version:    1,
		Deleted:    false,
		CreatedAt:  time.Now().Unix(),
	}

	// 查找或创建节点
	node := sl.findOrCreateNode(key)

	// 如果节点已存在，更新版本信息
	if len(node.versions) > 0 {
		lastVersion := node.versions[len(node.versions)-1]
		versionedKV.CreatedRev = lastVersion.CreatedRev
		versionedKV.Version = lastVersion.Version + 1
	}

	// 添加新版本到历史
	node.versions = append(node.versions, versionedKV)

	return revision, nil
}

// findOrCreateNode 查找或创建节点
func (sl *SkipList) findOrCreateNode(key string) *Node {
	update := make([]*Node, maxLevel)
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
		return x // 节点已存在
	}

	// 创建新节点
	lvl := sl.randomLevel()
	if lvl > sl.level {
		for i := sl.level; i < lvl; i++ {
			update[i] = sl.header
		}
		sl.level = lvl
	}

	newNode := &Node{
		key:      key,
		versions: make([]*VersionedKV, 0),
		next:     make([]*Node, lvl),
	}

	for i := 0; i < lvl; i++ {
		newNode.next[i] = update[i].next[i]
		update[i].next[i] = newNode
	}

	return newNode
}

// Get 获取键值对，支持版本查询
func (sl *SkipList) Get(key string, revision int64) (*VersionedKV, error) {
	sl.mu.RLock()
	defer sl.mu.RUnlock()

	node := sl.findNode(key)
	if node == nil {
		return nil, ErrKeyNotFound
	}

	// 如果指定版本号，查找该版本或之前的版本
	if revision > 0 {
		if revision < sl.compactedRev {
			return nil, ErrRevisionCompacted
		}
		return sl.getAtRevision(node, revision)
	}

	// 返回最新版本
	if len(node.versions) == 0 {
		return nil, ErrKeyNotFound
	}

	latest := node.versions[len(node.versions)-1]

	// 检查是否已删除
	if latest.Deleted {
		return nil, ErrKeyDeleted
	}

	// 检查TTL
	if latest.TTL > 0 {
		now := time.Now().Unix()
		if now-latest.CreatedAt >= latest.TTL {
			return nil, ErrKeyExpired
		}
	}

	return latest, nil
}

// findNode 查找节点
func (sl *SkipList) findNode(key string) *Node {
	x := sl.header
	for i := sl.level - 1; i >= 0; i-- {
		for x.next[i] != nil && x.next[i].key < key {
			x = x.next[i]
		}
	}
	x = x.next[0]
	if x != nil && x.key == key {
		return x
	}
	return nil
}

// getAtRevision 获取指定版本号的键值对
func (sl *SkipList) getAtRevision(node *Node, revision int64) (*VersionedKV, error) {
	// 二分查找最接近但不大于指定版本号的版本
	left, right := 0, len(node.versions)-1
	var target *VersionedKV

	for left <= right {
		mid := (left + right) / 2
		version := node.versions[mid]

		if version.ModRev <= revision {
			target = version
			left = mid + 1
		} else {
			right = mid - 1
		}
	}

	if target == nil {
		return nil, ErrKeyNotFound
	}

	// 检查是否已删除
	if target.Deleted {
		return nil, ErrKeyDeleted
	}

	// 检查TTL
	if target.TTL > 0 {
		now := time.Now().Unix()
		if now-target.CreatedAt >= target.TTL {
			return nil, ErrKeyExpired
		}
	}

	return target, nil
}

// Delete 删除键，创建删除版本
func (sl *SkipList) Delete(key string) (int64, error) {
	sl.mu.Lock()
	defer sl.mu.Unlock()

	// 分配新版本号
	revision := sl.currentRevision
	sl.currentRevision++

	node := sl.findNode(key)
	if node == nil {
		return revision, nil // 键不存在，删除操作成功
	}

	// 创建删除版本
	deleteVersion := &VersionedKV{
		Key:        key,
		Value:      "",
		TTL:        0,
		CreatedRev: node.versions[0].CreatedRev,
		ModRev:     revision,
		Version:    int64(len(node.versions) + 1),
		Deleted:    true,
		CreatedAt:  time.Now().Unix(),
	}

	// 添加到版本历史
	node.versions = append(node.versions, deleteVersion)

	return revision, nil
}

// Range 范围查询，支持版本查询
func (sl *SkipList) Range(start, end string, revision int64, limit int64) ([]*VersionedKV, int64, error) {
	sl.mu.RLock()
	defer sl.mu.RUnlock()

	var results []*VersionedKV
	count := int64(0)

	// 定位到 >= start 的节点
	x := sl.header
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

		if limit > 0 && count >= limit {
			break
		}

		var versionedKV *VersionedKV
		var err error

		if revision > 0 {
			versionedKV, err = sl.getAtRevision(x, revision)
		} else {
			versionedKV, err = sl.Get(x.key, 0)
		}

		if err == nil {
			results = append(results, versionedKV)
		}
		// 忽略错误，继续处理其他键

		count++
		x = x.next[0]
	}

	return results, sl.currentRevision - 1, nil
}

// GetHistory 获取键的版本历史
func (sl *SkipList) GetHistory(key string, limit int64) ([]*VersionedKV, error) {
	sl.mu.RLock()
	defer sl.mu.RUnlock()

	node := sl.findNode(key)
	if node == nil {
		return nil, ErrKeyNotFound
	}

	versions := make([]*VersionedKV, len(node.versions))
	copy(versions, node.versions)

	// 限制返回的版本数量
	if limit > 0 && int64(len(versions)) > limit {
		versions = versions[len(versions)-int(limit):]
	}

	return versions, nil
}

// Compact 压缩版本历史
func (sl *SkipList) Compact(revision int64) (int64, error) {
	sl.mu.Lock()
	defer sl.mu.Unlock()

	if revision <= sl.compactedRev {
		return sl.compactedRev, nil
	}

	if revision >= sl.currentRevision {
		return 0, ErrRevisionTooHigh
	}

	compacted := 0
	nodesToDelete := make([]*Node, 0)

	// 遍历所有节点，压缩版本历史
	x := sl.header.next[0]
	for x != nil {
		next := x.next[0] // 保存下一个节点，因为当前节点可能被删除

		// 找到需要保留的版本索引
		keepIndex := -1
		for i, version := range x.versions {
			if version.ModRev > revision {
				keepIndex = i - 1
				break
			}
		}

		if keepIndex == -1 {
			// 所有版本都可以删除
			nodesToDelete = append(nodesToDelete, x)
			compacted += len(x.versions)
		} else if keepIndex >= 0 {
			// 保留部分版本
			x.versions = x.versions[keepIndex:]
			compacted += keepIndex
		}

		x = next
	}

	// 删除需要删除的节点
	for _, node := range nodesToDelete {
		sl.deleteNode(node.key)
	}

	sl.compactedRev = revision
	return revision, nil
}

// deleteNode 删除节点
func (sl *SkipList) deleteNode(key string) bool {
	update := make([]*Node, maxLevel)
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

// GetStats 获取统计信息
func (sl *SkipList) GetStats() map[string]interface{} {
	sl.mu.RLock()
	defer sl.mu.RUnlock()

	totalKeys := 0
	totalVersions := 0

	x := sl.header.next[0]
	for x != nil {
		totalKeys++
		totalVersions += len(x.versions)
		x = x.next[0]
	}

	return map[string]interface{}{
		"current_revision":   sl.currentRevision - 1,
		"compacted_revision": sl.compactedRev,
		"total_keys":         totalKeys,
		"total_versions":     totalVersions,
		"level":              sl.level,
	}
}

// CleanupExpired 清理过期的键值对
func (sl *SkipList) CleanupExpired() int {
	expiredKeys := sl.GetExpiredKeys()

	sl.mu.Lock()
	defer sl.mu.Unlock()

	// 删除过期节点
	for _, key := range expiredKeys {
		sl.deleteNode(key)
	}

	return len(expiredKeys)
}

// GetExpiredKeys 获取过期的键列表
func (sl *SkipList) GetExpiredKeys() []string {
	sl.mu.RLock()
	defer sl.mu.RUnlock()

	var expiredKeys []string
	now := time.Now().Unix()

	x := sl.header.next[0]
	for x != nil {
		// 检查最新版本是否过期
		if len(x.versions) > 0 {
			latest := x.versions[len(x.versions)-1]
			if latest.TTL > 0 && now-latest.CreatedAt >= latest.TTL {
				expiredKeys = append(expiredKeys, x.key)
			}
		}
		x = x.next[0]
	}

	return expiredKeys
}

// GetAllWithTTL 获取所有键值对（包括TTL信息），用于快照
func (sl *SkipList) GetAllWithTTL() []KVPair {
	sl.mu.RLock()
	defer sl.mu.RUnlock()

	var res []KVPair
	now := time.Now().Unix()

	x := sl.header.next[0]
	for x != nil {
		if len(x.versions) > 0 {
			latest := x.versions[len(x.versions)-1]

			// 跳过已删除的键
			if latest.Deleted {
				x = x.next[0]
				continue
			}

			// 跳过过期的键
			if latest.TTL > 0 && now-latest.CreatedAt >= latest.TTL {
				x = x.next[0]
				continue
			}

			res = append(res, KVPair{
				Key:   x.key,
				Value: latest.Value,
				TTL:   latest.TTL,
			})
		}
		x = x.next[0]
	}

	return res
}

// AllNodes 返回所有节点（用于快照保存所有历史版本）
func (sl *SkipList) AllNodes() []*Node {
	sl.mu.RLock()
	defer sl.mu.RUnlock()
	var nodes []*Node
	x := sl.header.next[0]
	for x != nil {
		nodes = append(nodes, x)
		x = x.next[0]
	}
	return nodes
}

// RestoreVersion 直接插入一个历史版本（用于快照恢复）
func (sl *SkipList) RestoreVersion(version *VersionedKV) {
	sl.mu.Lock()
	defer sl.mu.Unlock()

	node := sl.findOrCreateNode(version.Key)
	// 保证历史版本按ModRev递增插入
	inserted := false
	for i, v := range node.versions {
		if version.ModRev < v.ModRev {
			// 插入到第i个位置
			node.versions = append(node.versions[:i], append([]*VersionedKV{version}, node.versions[i:]...)...)
			inserted = true
			break
		}
	}
	if !inserted {
		node.versions = append(node.versions, version)
	}

	// 更新currentRevision和compactedRev
	if version.ModRev >= sl.currentRevision {
		sl.currentRevision = version.ModRev + 1
	}
	if version.ModRev < sl.compactedRev || sl.compactedRev == 0 {
		sl.compactedRev = version.ModRev
	}
}

// 错误定义
var (
	ErrKeyNotFound       = &KVError{"key not found"}
	ErrKeyDeleted        = &KVError{"key has been deleted"}
	ErrKeyExpired        = &KVError{"key has expired"}
	ErrRevisionCompacted = &KVError{"revision has been compacted"}
	ErrRevisionTooHigh   = &KVError{"revision is greater than current revision"}
)

// KVError 自定义错误类型
type KVError struct {
	message string
}

func (e *KVError) Error() string {
	return e.message
}
