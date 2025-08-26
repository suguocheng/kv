# KV系统 Makefile
# 提供常用的构建、测试和运行命令

.PHONY: help build test test-unit test-functional test-integration test-cluster test-benchmark test-all clean

# 默认目标
help:
	@echo "KV系统构建和测试工具"
	@echo ""
	@echo "可用命令:"
	@echo "  build          - 构建项目"
	@echo "  test           - 运行所有测试"
	@echo "  test-all       - 运行完整测试套件 (按测试金字塔顺序)"
	@echo ""
	@echo "测试命令 (按测试金字塔顺序):"
	@echo "  test-unit      - 运行单元测试 (第1阶段 - 最基础)"
	@echo "  test-integration - 运行集成测试 (第2阶段 - 组件交互)"
	@echo "  test-cluster    - 运行集群测试 (第3阶段 - 分布式功能)"
	@echo "  test-e2e       - 运行端到端测试 (第4阶段 - 完整场景)"
	@echo "  test-benchmark - 运行性能基准测试 (第5阶段 - 最耗时)"
	@echo ""
	@echo "其他命令:"
	@echo "  clean          - 清理构建文件"
	@echo "  server         - 启动服务器"
	@echo "  client         - 启动客户端"
	@echo "  stop           - 停止所有服务器"

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
	go run ./cmd/client/main.go ./cmd/client/handlers.go ./cmd/client/help.go

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
	rm -rf test/logs/
	rm -rf test/reports/
	rm -rf test/benchmarks/
	rm -rf test/final_reports/
	go clean

# 安装依赖
deps:
	@echo "安装Go依赖..."
	go mod download
	go mod tidy

# 格式化代码
fmt:
	@echo "格式化Go代码..."
	go fmt ./...

# 代码检查
lint:
	@echo "运行代码检查..."
	@if command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run; \
	else \
		echo "golangci-lint 未安装，跳过代码检查"; \
	fi

# 生成协议缓冲区代码
proto:
	@echo "生成协议缓冲区代码..."
	@if command -v protoc >/dev/null 2>&1; then \
		protoc --go_out=. --go_opt=paths=source_relative \
			--go-grpc_out=. --go-grpc_opt=paths=source_relative \
			pkg/proto/kvpb/*.proto; \
	else \
		echo "protoc 未安装，跳过协议缓冲区代码生成"; \
	fi

# 显示测试覆盖率
coverage:
	@echo "生成测试覆盖率报告..."
	go test -coverprofile=coverage.out ./pkg/...
	go tool cover -html=coverage.out -o coverage.html
	@echo "覆盖率报告已生成: coverage.html"

# 运行基准测试
bench:
	@echo "运行Go基准测试..."
	go test -bench=. -benchmem ./pkg/...

# 检查代码质量
check: fmt lint test-go
	@echo "代码质量检查完成"

# 完整构建流程
all: deps fmt lint build test-all
	@echo "完整构建流程完成"

# 开发环境设置
dev-setup: deps proto
	@echo "开发环境设置完成"
	@echo "请复制 config.example.env 为 config.env 并编辑配置"

# 显示系统信息
info:
	@echo "系统信息:"
	@echo "  Go版本: $(shell go version)"
	@echo "  操作系统: $(shell uname -s)"
	@echo "  架构: $(shell uname -m)"
	@echo "  CPU核心数: $(shell nproc)"
	@echo "  内存: $(shell free -h | awk 'NR==2{print $2}')" 