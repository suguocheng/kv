package client

import (
	"context"
	"fmt"
	"sync"
	"time"

	"kv/pkg/proto/kvpb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

// ConnectionPool gRPC连接池
type ConnectionPool struct {
	mu      sync.RWMutex
	conns   map[string][]*grpc.ClientConn     // 按地址分组的连接
	clients map[string][]kvpb.KVServiceClient // 按地址分组的客户端
	config  *PoolConfig
}

// PoolConfig 连接池配置
type PoolConfig struct {
	MaxConnectionsPerHost int           // 每个主机的最大连接数
	MinConnectionsPerHost int           // 每个主机的最小连接数
	MaxIdleTime           time.Duration // 连接最大空闲时间
	MaxLifetime           time.Duration // 连接最大生命周期
	DialTimeout           time.Duration // 连接超时时间
	KeepAliveTime         time.Duration // 保活时间
	KeepAliveTimeout      time.Duration // 保活超时时间
}

// DefaultPoolConfig 默认连接池配置
func DefaultPoolConfig() *PoolConfig {
	return &PoolConfig{
		MaxConnectionsPerHost: 10,
		MinConnectionsPerHost: 2,
		MaxIdleTime:           30 * time.Minute,
		MaxLifetime:           1 * time.Hour,
		DialTimeout:           1 * time.Second, // 减少超时时间，避免测试时等待太久
		KeepAliveTime:         30 * time.Second,
		KeepAliveTimeout:      5 * time.Second,
	}
}

// TestPoolConfig 测试环境连接池配置
func TestPoolConfig() *PoolConfig {
	return &PoolConfig{
		MaxConnectionsPerHost: 5,
		MinConnectionsPerHost: 0, // 测试时不预创建连接
		MaxIdleTime:           5 * time.Minute,
		MaxLifetime:           10 * time.Minute,
		DialTimeout:           100 * time.Millisecond, // 更短的超时时间
		KeepAliveTime:         10 * time.Second,
		KeepAliveTimeout:      2 * time.Second,
	}
}

// NewConnectionPool 创建新的连接池
func NewConnectionPool(config *PoolConfig) *ConnectionPool {
	if config == nil {
		config = DefaultPoolConfig()
	}

	pool := &ConnectionPool{
		conns:   make(map[string][]*grpc.ClientConn),
		clients: make(map[string][]kvpb.KVServiceClient),
		config:  config,
	}

	// 启动连接清理协程
	go pool.cleanupIdleConnections()

	return pool
}

// GetClient 从连接池获取客户端
func (p *ConnectionPool) GetClient(addr string) (kvpb.KVServiceClient, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// 检查是否有可用连接
	if clients, exists := p.clients[addr]; exists && len(clients) > 0 {
		// 使用轮询方式选择连接
		client := clients[0]
		// 将使用的连接移到末尾（简单的轮询）
		p.clients[addr] = append(clients[1:], client)
		return client, nil
	}

	// 创建新连接
	conn, err := p.createConnection(addr)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection to %s: %v", addr, err)
	}

	client := kvpb.NewKVServiceClient(conn)

	// 添加到连接池
	p.conns[addr] = append(p.conns[addr], conn)
	p.clients[addr] = append(p.clients[addr], client)

	return client, nil
}

// createConnection 创建新的gRPC连接
func (p *ConnectionPool) createConnection(addr string) (*grpc.ClientConn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), p.config.DialTimeout)
	defer cancel()

	conn, err := grpc.DialContext(ctx, addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                p.config.KeepAliveTime,
			Timeout:             p.config.KeepAliveTimeout,
			PermitWithoutStream: true,
		}),
		grpc.WithBlock(),
	)

	if err != nil {
		return nil, err
	}

	return conn, nil
}

// AddServer 添加服务器到连接池
func (p *ConnectionPool) AddServer(addr string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// 如果已经存在，直接返回
	if _, exists := p.conns[addr]; exists {
		return nil
	}

	// 创建初始连接
	successCount := 0
	for i := 0; i < p.config.MinConnectionsPerHost; i++ {
		conn, err := p.createConnection(addr)
		if err != nil {
			// 在测试环境中，允许部分连接失败
			fmt.Printf("Warning: failed to create initial connection %d to %s: %v\n", i, addr, err)
			continue
		}

		client := kvpb.NewKVServiceClient(conn)
		p.conns[addr] = append(p.conns[addr], conn)
		p.clients[addr] = append(p.clients[addr], client)
		successCount++
	}

	// 如果没有任何连接成功，返回错误
	if successCount == 0 {
		return fmt.Errorf("failed to create any initial connections to %s", addr)
	}

	return nil
}

// RemoveServer 从连接池移除服务器
func (p *ConnectionPool) RemoveServer(addr string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if conns, exists := p.conns[addr]; exists {
		// 关闭所有连接
		for _, conn := range conns {
			conn.Close()
		}
		delete(p.conns, addr)
		delete(p.clients, addr)
	}
}

// cleanupIdleConnections 清理空闲连接
func (p *ConnectionPool) cleanupIdleConnections() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		p.mu.Lock()

		for addr, conns := range p.conns {
			if len(conns) > p.config.MinConnectionsPerHost {
				// 保留最小连接数，关闭多余的连接
				excess := len(conns) - p.config.MinConnectionsPerHost
				for i := 0; i < excess; i++ {
					if len(conns) > 0 {
						conn := conns[len(conns)-1]
						conn.Close()
						p.conns[addr] = conns[:len(conns)-1]
						p.clients[addr] = p.clients[addr][:len(p.clients[addr])-1]
					}
				}
			}
		}

		p.mu.Unlock()
	}
}

// Close 关闭连接池
func (p *ConnectionPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for addr, conns := range p.conns {
		for _, conn := range conns {
			conn.Close()
		}
		delete(p.conns, addr)
		delete(p.clients, addr)
	}
}

// GetStats 获取连接池统计信息
func (p *ConnectionPool) GetStats() map[string]interface{} {
	p.mu.RLock()
	defer p.mu.RUnlock()

	stats := make(map[string]interface{})
	totalConns := 0

	for addr, conns := range p.conns {
		stats[addr] = map[string]interface{}{
			"connections": len(conns),
			"clients":     len(p.clients[addr]),
		}
		totalConns += len(conns)
	}

	stats["total_connections"] = totalConns
	stats["total_servers"] = len(p.conns)

	return stats
}
