package kvstore

import (
	"fmt"
	"sync"
	"testing"
)

// BenchmarkSkipListPut 单线程Put操作基准测试
func BenchmarkSkipListPut(b *testing.B) {
	sl := NewSkipList()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bench_key_%d", i)
		value := fmt.Sprintf("bench_value_%d", i)
		sl.Put(key, value, 0)
	}
}

// BenchmarkSkipListGet 单线程Get操作基准测试
func BenchmarkSkipListGet(b *testing.B) {
	sl := NewSkipList()

	// 预填充数据
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("bench_key_%d", i)
		value := fmt.Sprintf("bench_value_%d", i)
		sl.Put(key, value, 0)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bench_key_%d", i%1000)
		sl.Get(key, 0)
	}
}

// BenchmarkSkipListConcurrentPut 并发Put操作基准测试
func BenchmarkSkipListConcurrentPut(b *testing.B) {
	sl := NewSkipList()

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("concurrent_key_%d", i)
			value := fmt.Sprintf("concurrent_value_%d", i)
			sl.Put(key, value, 0)
			i++
		}
	})
}

// BenchmarkSkipListConcurrentGet 并发Get操作基准测试
func BenchmarkSkipListConcurrentGet(b *testing.B) {
	sl := NewSkipList()

	// 预填充数据
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("concurrent_key_%d", i)
		value := fmt.Sprintf("concurrent_value_%d", i)
		sl.Put(key, value, 0)
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("concurrent_key_%d", i%1000)
			sl.Get(key, 0)
			i++
		}
	})
}

// BenchmarkSkipListMixedOperations 混合操作基准测试
func BenchmarkSkipListMixedOperations(b *testing.B) {
	sl := NewSkipList()

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("mixed_key_%d", i)

			if i%3 == 0 {
				// Put操作
				value := fmt.Sprintf("mixed_value_%d", i)
				sl.Put(key, value, 0)
			} else {
				// Get操作
				sl.Get(key, 0)
			}
			i++
		}
	})
}

// BenchmarkSkipListVersionAccess 版本访问基准测试
func BenchmarkSkipListVersionAccess(b *testing.B) {
	sl := NewSkipList()
	key := "version_key"

	// 创建多个版本
	for i := 0; i < 100; i++ {
		sl.Put(key, fmt.Sprintf("value_%d", i), 0)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		revision := int64((i % 100) + 1)
		sl.Get(key, revision)
	}
}

// BenchmarkSkipListConcurrentVersionAccess 并发版本访问基准测试
func BenchmarkSkipListConcurrentVersionAccess(b *testing.B) {
	sl := NewSkipList()
	key := "concurrent_version_key"

	// 创建多个版本
	for i := 0; i < 100; i++ {
		sl.Put(key, fmt.Sprintf("value_%d", i), 0)
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			revision := int64((i % 100) + 1)
			sl.Get(key, revision)
			i++
		}
	})
}

// BenchmarkSkipListDelete 删除操作基准测试
func BenchmarkSkipListDelete(b *testing.B) {
	sl := NewSkipList()

	// 预填充数据
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("delete_key_%d", i)
		value := fmt.Sprintf("delete_value_%d", i)
		sl.Put(key, value, 0)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("delete_key_%d", i)
		sl.Delete(key)
	}
}

// BenchmarkSkipListTTL 带TTL的操作基准测试
func BenchmarkSkipListTTL(b *testing.B) {
	sl := NewSkipList()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("ttl_key_%d", i)
		value := fmt.Sprintf("ttl_value_%d", i)
		sl.Put(key, value, 60) // 60秒TTL
	}
}

// BenchmarkSkipListSegmentLockDistribution 分段锁分布基准测试
func BenchmarkSkipListSegmentLockDistribution(b *testing.B) {
	sl := NewSkipList()

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			// 使用不同的key模式来测试分段分布
			key := fmt.Sprintf("segment_key_%d", i)
			value := fmt.Sprintf("segment_value_%d", i)
			sl.Put(key, value, 0)
			i++
		}
	})
}

// BenchmarkSkipListLargeData 大数据操作基准测试
func BenchmarkSkipListLargeData(b *testing.B) {
	sl := NewSkipList()

	// 创建1KB的数据
	largeValue := make([]byte, 1024)
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("large_key_%d", i)
		sl.Put(key, string(largeValue), 0)
	}
}

// BenchmarkSkipListStress 压力测试基准测试
func BenchmarkSkipListStress(b *testing.B) {
	sl := NewSkipList()
	numGoroutines := 10
	numOperations := b.N / numGoroutines

	var wg sync.WaitGroup

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				key := fmt.Sprintf("stress_key_%d_%d", id, j)
				value := fmt.Sprintf("stress_value_%d_%d", id, j)
				sl.Put(key, value, 0)
				sl.Get(key, 0)
			}
		}(i)
	}

	wg.Wait()
}
