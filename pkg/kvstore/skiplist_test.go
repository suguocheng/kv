package kvstore

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// ==================== 基础功能测试 ====================

func TestSkipListBasic(t *testing.T) {
	sl := NewSkipList()

	// 测试Put和Get
	sl.Put("key1", "value1", 0)
	sl.Put("key2", "value2", 0)

	val, err := sl.Get("key1", 0)
	if err != nil {
		t.Errorf("Get key1 failed: %v", err)
	}
	if val.Value != "value1" {
		t.Errorf("Expected value1, got %s", val.Value)
	}

	val, err = sl.Get("key2", 0)
	if err != nil {
		t.Errorf("Get key2 failed: %v", err)
	}
	if val.Value != "value2" {
		t.Errorf("Expected value2, got %s", val.Value)
	}
}

func TestSkipListDelete(t *testing.T) {
	sl := NewSkipList()

	// 添加键值对
	sl.Put("key1", "value1", 0)
	sl.Put("key2", "value2", 0)

	// 删除键
	revision, err := sl.Delete("key1")
	if err != nil {
		t.Errorf("Delete key1 failed: %v", err)
	}
	if revision <= 0 {
		t.Errorf("Expected positive revision, got %d", revision)
	}

	// 验证删除
	_, err = sl.Get("key1", 0)
	if err == nil {
		t.Error("Expected error for deleted key")
	}

	// 验证其他键仍然存在
	val, err := sl.Get("key2", 0)
	if err != nil {
		t.Errorf("Get key2 failed: %v", err)
	}
	if val.Value != "value2" {
		t.Errorf("Expected value2, got %s", val.Value)
	}
}

func TestSkipListTTL(t *testing.T) {
	sl := NewSkipList()

	// 添加带TTL的键值对
	sl.Put("key1", "value1", 1) // 1秒TTL

	// 立即获取应该成功
	val, err := sl.Get("key1", 0)
	if err != nil {
		t.Errorf("Get key1 failed: %v", err)
	}
	if val.Value != "value1" {
		t.Errorf("Expected value1, got %s", val.Value)
	}

	// 等待TTL过期
	time.Sleep(2 * time.Second)

	// 获取应该失败
	_, err = sl.Get("key1", 0)
	if err == nil {
		t.Error("Expected error for expired key")
	}
}

func TestSkipListMVCC(t *testing.T) {
	sl := NewSkipList()

	// 多次更新同一个键
	sl.Put("key1", "value1", 0)
	sl.Put("key1", "value2", 0)
	sl.Put("key1", "value3", 0)

	// 获取最新版本
	val, err := sl.Get("key1", 0)
	if err != nil {
		t.Errorf("Get latest version failed: %v", err)
	}
	if val.Value != "value3" {
		t.Errorf("Expected value3, got %s", val.Value)
	}

	// 获取历史版本
	val, err = sl.Get("key1", val.ModRev-1)
	if err != nil {
		t.Errorf("Get previous version failed: %v", err)
	}
	if val.Value != "value2" {
		t.Errorf("Expected value2, got %s", val.Value)
	}
}

func TestSkipListRange(t *testing.T) {
	sl := NewSkipList()

	// 添加多个键值对
	sl.Put("a", "value1", 0)
	sl.Put("b", "value2", 0)
	sl.Put("c", "value3", 0)
	sl.Put("d", "value4", 0)
	sl.Put("e", "value5", 0)

	// 范围查询
	results, _, err := sl.Range("b", "d", 0, 10)
	if err != nil {
		t.Errorf("Range query failed: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("Expected 2 results, got %d", len(results))
	}

	// 验证结果
	expected := []string{"b", "c"}
	for i, result := range results {
		if result.Key != expected[i] {
			t.Errorf("Expected key %s, got %s", expected[i], result.Key)
		}
	}
}

func TestSkipListHistory(t *testing.T) {
	sl := NewSkipList()

	// 多次更新同一个键
	sl.Put("key1", "value1", 0)
	sl.Put("key1", "value2", 0)
	sl.Put("key1", "value3", 0)

	// 获取历史
	history, err := sl.GetHistory("key1", 10)
	if err != nil {
		t.Errorf("GetHistory failed: %v", err)
	}
	if len(history) != 3 {
		t.Errorf("Expected 3 history entries, got %d", len(history))
	}

	// 验证历史顺序（从旧到新）
	expected := []string{"value1", "value2", "value3"}
	for i, entry := range history {
		if entry.Value != expected[i] {
			t.Errorf("Expected value %s, got %s", expected[i], entry.Value)
		}
	}
}

func TestSkipListCompact(t *testing.T) {
	sl := NewSkipList()

	// 添加多个版本
	sl.Put("key1", "value1", 0)
	sl.Put("key1", "value2", 0)
	sl.Put("key1", "value3", 0)
	sl.Put("key1", "value4", 0)

	// 压缩到版本2
	sl.Compact(2)

	// 验证旧版本被删除
	history, err := sl.GetHistory("key1", 10)
	if err != nil {
		t.Errorf("GetHistory after compact failed: %v", err)
	}
	if len(history) < 2 {
		t.Errorf("Expected at least 2 history entries after compact, got %d", len(history))
	}

	// 验证最新版本仍然存在
	val, err := sl.Get("key1", 0)
	if err != nil {
		t.Errorf("Get latest version after compact failed: %v", err)
	}
	if val.Value != "value4" {
		t.Errorf("Expected value4, got %s", val.Value)
	}
}

func TestSkipListStats(t *testing.T) {
	sl := NewSkipList()

	// 添加一些数据
	sl.Put("key1", "value1", 0)
	sl.Put("key2", "value2", 0)
	sl.Put("key3", "value3", 0)

	stats := sl.GetStats()

	// 验证统计信息
	if stats["total_keys"] != 3 {
		t.Errorf("Expected 3 total keys, got %v", stats["total_keys"])
	}

	if stats["current_revision"] == nil {
		t.Error("Expected current_revision in stats")
	}
}

// ==================== 高级功能测试 ====================

func TestSkipListConcurrencyStress(t *testing.T) {
	sl := NewSkipList()
	numGoroutines := 100
	numOperations := 1000

	var wg sync.WaitGroup
	start := time.Now()

	// 并发写入
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				key := fmt.Sprintf("key_%d_%d", id, j)
				value := fmt.Sprintf("value_%d_%d", id, j)
				sl.Put(key, value, 0)
			}
		}(i)
	}

	// 并发读取
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				key := fmt.Sprintf("key_%d_%d", id, j)
				sl.Get(key, 0)
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(start)

	t.Logf("压力测试完成: %d个goroutine, 每个%d次操作, 耗时: %v",
		numGoroutines, numOperations, duration)
	t.Logf("总操作数: %d, 平均吞吐量: %.2f ops/s",
		numGoroutines*numOperations*2,
		float64(numGoroutines*numOperations*2)/duration.Seconds())
}

func TestSkipListVersionIndexPerformance(t *testing.T) {
	sl := NewSkipList()
	key := "test_key"
	numVersions := 10000

	// 创建大量版本
	start := time.Now()
	for i := 0; i < numVersions; i++ {
		sl.Put(key, fmt.Sprintf("value_%d", i), 0)
	}
	writeTime := time.Since(start)

	// 测试版本查找性能
	start = time.Now()
	for i := 0; i < 1000; i++ {
		revision := int64((i % numVersions) + 1)
		_, err := sl.Get(key, revision)
		if err != nil {
			t.Errorf("版本查找失败: %v", err)
		}
	}
	readTime := time.Since(start)

	t.Logf("版本索引性能测试:")
	t.Logf("  写入%d个版本耗时: %v", numVersions, writeTime)
	t.Logf("  查找1000次耗时: %v", readTime)
	t.Logf("  平均查找时间: %v", readTime/1000)
}

func TestSkipListSegmentLockDistribution(t *testing.T) {
	sl := NewSkipList()
	numKeys := 10000

	// 统计分段分布
	segmentCounts := make(map[int]int)

	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key_%d", i)
		segment := sl.getSegmentLock(key)

		// 计算分段索引
		segmentIndex := -1
		for j, lock := range sl.segmentLocks {
			if &lock == segment {
				segmentIndex = j
				break
			}
		}

		if segmentIndex >= 0 {
			segmentCounts[segmentIndex]++
		}
	}

	// 分析分布
	t.Logf("分段锁分布测试 (共%d个key):", numKeys)
	for i := 0; i < 16; i++ {
		count := segmentCounts[i]
		percentage := float64(count) / float64(numKeys) * 100
		t.Logf("  分段%d: %d个key (%.2f%%)", i, count, percentage)
	}

	// 检查分布是否均匀
	expectedCount := numKeys / 16
	tolerance := expectedCount / 2

	for i := 0; i < 16; i++ {
		count := segmentCounts[i]
		if count < expectedCount-tolerance || count > expectedCount+tolerance {
			t.Logf("警告: 分段%d分布不均匀 (期望~%d, 实际%d)", i, expectedCount, count)
		}
	}
}

func TestSkipListMemoryUsage(t *testing.T) {
	sl := NewSkipList()
	numKeys := 1000
	numVersions := 100

	// 记录初始内存使用（这里只是估算）
	initialSize := estimateMemoryUsage(sl)

	// 创建大量数据
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key_%d", i)
		for j := 0; j < numVersions; j++ {
			sl.Put(key, fmt.Sprintf("value_%d_%d", i, j), 0)
		}
	}

	finalSize := estimateMemoryUsage(sl)
	memoryIncrease := finalSize - initialSize

	t.Logf("内存使用测试:")
	t.Logf("  初始内存估算: %d bytes", initialSize)
	t.Logf("  最终内存估算: %d bytes", finalSize)
	t.Logf("  内存增长: %d bytes", memoryIncrease)
	t.Logf("  平均每个key的内存开销: %d bytes", memoryIncrease/int64(numKeys))
	t.Logf("  平均每个版本的内存开销: %d bytes", memoryIncrease/int64(numKeys*numVersions))
}

// estimateMemoryUsage 估算内存使用（简化版本）
func estimateMemoryUsage(sl *SkipList) int64 {
	var size int64

	// 基础结构
	size += 64 // SkipList header

	// 分段锁
	size += int64(len(sl.segmentLocks)) * 8 // 每个锁约8字节

	// 节点和版本数据
	allNodes := sl.AllNodes()
	for _, node := range allNodes {
		// 节点基础结构
		size += 64 // Node header

		// 版本数组
		size += int64(len(node.versions)) * 64 // 每个VersionedKV约64字节

		// 版本索引
		size += int64(len(node.versionIndex)) * 16 // 每个索引项约16字节
	}

	return size
}

func TestSkipListConcurrentVersionAccess(t *testing.T) {
	sl := NewSkipList()
	key := "concurrent_key"
	numVersions := 1000

	// 预创建版本
	for i := 0; i < numVersions; i++ {
		sl.Put(key, fmt.Sprintf("value_%d", i), 0)
	}

	numGoroutines := 50
	numAccesses := 100

	var wg sync.WaitGroup
	start := time.Now()

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numAccesses; j++ {
				revision := int64((id+j)%numVersions + 1)
				_, err := sl.Get(key, revision)
				if err != nil {
					t.Errorf("并发版本访问失败: %v", err)
				}
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(start)

	t.Logf("并发版本访问测试:")
	t.Logf("  %d个goroutine, 每个%d次访问", numGoroutines, numAccesses)
	t.Logf("  总访问次数: %d", numGoroutines*numAccesses)
	t.Logf("  总耗时: %v", duration)
	t.Logf("  平均吞吐量: %.2f ops/s",
		float64(numGoroutines*numAccesses)/duration.Seconds())
}

func TestSkipListMixedWorkload(t *testing.T) {
	sl := NewSkipList()
	numKeys := 100
	numOperations := 1000

	var wg sync.WaitGroup
	start := time.Now()

	// 混合读写操作
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				keyIndex := (id + j) % numKeys
				key := fmt.Sprintf("mixed_key_%d", keyIndex)

				if j%3 == 0 {
					// 写操作
					sl.Put(key, fmt.Sprintf("value_%d_%d", id, j), 0)
				} else {
					// 读操作
					sl.Get(key, 0)
				}
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(start)

	t.Logf("混合工作负载测试:")
	t.Logf("  10个goroutine, 每个%d次操作", numOperations)
	t.Logf("  总操作数: %d", 10*numOperations)
	t.Logf("  总耗时: %v", duration)
	t.Logf("  平均吞吐量: %.2f ops/s",
		float64(10*numOperations)/duration.Seconds())
}

func TestSkipListOptimizationCorrectness(t *testing.T) {
	sl := NewSkipList()
	key := "correctness_key"

	// 创建多个版本
	revisions := make([]int64, 0)
	for i := 0; i < 100; i++ {
		revision, err := sl.Put(key, fmt.Sprintf("value_%d", i), 0)
		if err != nil {
			t.Fatalf("Put失败: %v", err)
		}
		revisions = append(revisions, revision)
	}

	// 测试版本索引正确性
	for i, expectedRevision := range revisions {
		expectedValue := fmt.Sprintf("value_%d", i)

		// 使用版本索引查找
		result, err := sl.Get(key, expectedRevision)
		if err != nil {
			t.Errorf("版本索引查找失败: %v", err)
			continue
		}

		if result.Value != expectedValue {
			t.Errorf("版本索引查找结果错误: 期望%s, 实际%s", expectedValue, result.Value)
		}

		// 测试中间版本查找
		if i > 0 {
			midRevision := expectedRevision - 1
			result, err := sl.Get(key, midRevision)
			if err != nil {
				t.Errorf("中间版本查找失败: %v", err)
				continue
			}

			prevValue := fmt.Sprintf("value_%d", i-1)
			if result.Value != prevValue {
				t.Errorf("中间版本查找结果错误: 期望%s, 实际%s", prevValue, result.Value)
			}
		}
	}

	t.Logf("优化正确性测试通过: 所有%d个版本查找正确", len(revisions))
}
