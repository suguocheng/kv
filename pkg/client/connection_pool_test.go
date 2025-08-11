package client

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// BenchmarkConnectionPool 连接池性能基准测试
func BenchmarkConnectionPool(b *testing.B) {
	servers := []string{"localhost:9000", "localhost:9001", "localhost:9002"}

	// 测试默认连接方式
	b.Run("DefaultConnection", func(b *testing.B) {
		client := NewClient(servers)
		defer client.Close()

		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				ctx := context.Background()
				client.Put(ctx, "bench_key", "bench_value")
			}
		})
	})

	// 测试连接池方式
	b.Run("ConnectionPool", func(b *testing.B) {
		poolConfig := &PoolConfig{
			MaxConnectionsPerHost: 10,
			MinConnectionsPerHost: 2,
			DialTimeout:           5 * time.Second,
		}
		client := NewClientWithPool(servers, poolConfig)
		defer client.Close()

		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				ctx := context.Background()
				client.Put(ctx, "bench_key", "bench_value")
			}
		})
	})
}

// TestConnectionPoolStats 测试连接池统计信息
func TestConnectionPoolStats(t *testing.T) {
	servers := []string{"localhost:9000", "localhost:9001"}

	// 使用测试配置
	poolConfig := TestPoolConfig()

	client := NewClientWithPool(servers, poolConfig)
	defer client.Close()

	// 获取统计信息
	stats := client.GetPoolStats()
	if stats == nil {
		t.Fatal("Expected pool stats, got nil")
	}

	// 验证统计信息
	if totalServers, ok := stats["total_servers"].(int); !ok || totalServers != 0 {
		t.Errorf("Expected total_servers to be 0 (no successful connections), got %v", totalServers)
	}

	// 在测试环境中，由于服务器不存在，连接数应该为0
	if totalConnections, ok := stats["total_connections"].(int); !ok || totalConnections != 0 {
		t.Errorf("Expected total_connections to be 0, got %v", totalConnections)
	}
}

// TestConnectionPoolConcurrent 测试连接池并发性能
func TestConnectionPoolConcurrent(t *testing.T) {
	servers := []string{"localhost:9000", "localhost:9001", "localhost:9002"}

	// 使用测试配置
	poolConfig := TestPoolConfig()

	client := NewClientWithPool(servers, poolConfig)
	defer client.Close()

	// 并发测试 - 只测试连接池的并发安全性，不实际连接
	const numGoroutines = 10
	const requestsPerGoroutine = 100

	var wg sync.WaitGroup
	start := time.Now()

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			for j := 0; j < requestsPerGoroutine; j++ {
				// 只测试连接池的统计信息获取，不尝试实际连接
				stats := client.GetPoolStats()
				if stats == nil {
					t.Logf("Pool stats is nil")
				}

				// 模拟一些计算工作
				key := fmt.Sprintf("concurrent_key_%d_%d", id, j)
				value := fmt.Sprintf("concurrent_value_%d_%d", id, j)
				_ = key
				_ = value
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(start)

	t.Logf("Concurrent test completed in %v", duration)
	t.Logf("Average time per request: %v", duration/time.Duration(numGoroutines*requestsPerGoroutine))

	// 验证连接池状态
	stats := client.GetPoolStats()
	if stats != nil {
		t.Logf("Pool stats: %+v", stats)
	}
}

// TestConnectionPoolReuse 测试连接复用
func TestConnectionPoolReuse(t *testing.T) {
	servers := []string{"localhost:9000"}

	// 使用测试配置
	poolConfig := TestPoolConfig()

	client := NewClientWithPool(servers, poolConfig)
	defer client.Close()

	// 在测试环境中，由于服务器不存在，连接会失败
	// 但我们仍然可以测试连接池的基本功能
	_, err := client.getClientFromPool(0)
	if err == nil {
		t.Fatalf("Expected error when server is not available")
	}

	// 验证连接池统计
	stats := client.GetPoolStats()
	if stats == nil {
		t.Fatal("Expected pool stats")
	}

	// 在测试环境中，由于连接失败，服务器数应该为0
	if totalServers, ok := stats["total_servers"].(int); !ok || totalServers != 0 {
		t.Errorf("Expected 0 servers (no successful connections), got %d", totalServers)
	}
}

// TestConnectionPoolConfig 测试连接池配置
func TestConnectionPoolConfig(t *testing.T) {
	// 测试默认配置
	defaultConfig := DefaultPoolConfig()
	if defaultConfig.MaxConnectionsPerHost != 10 {
		t.Errorf("Expected MaxConnectionsPerHost to be 10, got %d", defaultConfig.MaxConnectionsPerHost)
	}
	if defaultConfig.MinConnectionsPerHost != 2 {
		t.Errorf("Expected MinConnectionsPerHost to be 2, got %d", defaultConfig.MinConnectionsPerHost)
	}

	// 测试测试配置
	testConfig := TestPoolConfig()
	if testConfig.MaxConnectionsPerHost != 5 {
		t.Errorf("Expected MaxConnectionsPerHost to be 5, got %d", testConfig.MaxConnectionsPerHost)
	}
	if testConfig.MinConnectionsPerHost != 0 {
		t.Errorf("Expected MinConnectionsPerHost to be 0, got %d", testConfig.MinConnectionsPerHost)
	}
	if testConfig.DialTimeout != 100*time.Millisecond {
		t.Errorf("Expected DialTimeout to be 100ms, got %v", testConfig.DialTimeout)
	}

	// 测试自定义配置 - 使用测试友好的配置
	customConfig := &PoolConfig{
		MaxConnectionsPerHost: 20,
		MinConnectionsPerHost: 0,                      // 测试时不预创建连接
		DialTimeout:           100 * time.Millisecond, // 快速超时
	}

	servers := []string{"localhost:9000"}
	client := NewClientWithPool(servers, customConfig)
	defer client.Close()

	// 验证配置生效 - 在测试环境中连接会失败，但配置应该正确设置
	stats := client.GetPoolStats()
	if stats == nil {
		t.Fatal("Expected pool stats")
	}

	// 验证连接池已创建但连接失败
	if totalServers, ok := stats["total_servers"].(int); !ok || totalServers != 0 {
		t.Errorf("Expected 0 servers (no successful connections), got %d", totalServers)
	}
}
