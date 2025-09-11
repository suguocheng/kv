# KV Makefile
# 提供常用的构建、测试和运行命令

.PHONY: build test test-unit test-functional test-integration test-cluster test-benchmark test-all clean

# 构建项目
build:
	@echo "构建KV系统..."
	go build -o bin/server ./cmd/server
	go build -o bin/client ./cmd/client
	@echo "构建完成"

# 运行单元测试
test-unit:
	@echo "运行单元测试..."
	cd test && ./unit_test.sh

# 运行集成测试
test-integration:
	@echo "运行集成测试..."
	cd test && ./integration_test.sh

# 运行集群测试
test-cluster:
	@echo "运行集群测试..."
	cd test && ./cluster_test.sh

# 运行端到端测试
test-e2e:
	@echo "运行端到端测试..."
	cd test && ./e2e_test.sh

# 运行性能基准测试
test-benchmark:
	@echo "运行性能基准测试..."
	cd test && ./benchmark_test.sh

# 运行所有测试
test-all:
	@echo "运行完整测试套件..."
	cd test && ./run_all_tests.sh

# 运行Go单元测试
test-go:
	@echo "运行Go单元测试..."
	go test -v ./pkg/...

# 启动服务器
server:
	@echo "启动KV服务器..."
	@if [ ! -f config.env ]; then \
		echo "错误: 配置文件 config.env 不存在"; \
		echo "请复制 config.example.env 为 config.env 并编辑配置"; \
		exit 1; \
	fi
	@cd scripts && ./start_servers.sh

# 启动客户端
client:
	@echo "启动KV客户端..."
	go run ./cmd/client

# 停止服务器
stop:
	@echo "停止KV服务器..."
	@cd scripts && ./stop_servers.sh

# 清理数据
clean-data:
	@echo "清理测试数据..."
	@cd scripts && ./clean_data.sh

# 清理构建文件
clean:
	@echo "清理构建文件..."
	rm -rf bin/
	rm -rf test/results/
	go clean

# 安装依赖
deps:
	@echo "安装Go依赖..."
	go mod download
	go mod tidy
