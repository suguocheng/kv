#!/bin/bash

# KV系统集群测试脚本
# 专门用于测试需要完整集群的raft和server模块

set -e

# 脚本路径
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
START_SCRIPT="$PROJECT_ROOT/scripts/start_servers.sh"
STOP_SCRIPT="$PROJECT_ROOT/scripts/stop_servers.sh"
CLEAN_DATA_SCRIPT="$PROJECT_ROOT/scripts/clean_data.sh"

# 导入输出格式工具
source "$(dirname "$0")/output_formatter.sh"

# 测试输出目录
TEST_DIR="$PROJECT_ROOT/test"
RESULTS_DIR="$TEST_DIR/results"
LOGS_DIR="$TEST_DIR/logs"
REPORTS_DIR="$TEST_DIR/reports"

# 确保目录存在
mkdir -p "$RESULTS_DIR" "$LOGS_DIR" "$REPORTS_DIR"

# 生成带时间戳的文件名
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
RESULT_FILE="$RESULTS_DIR/cluster_test_results_$TIMESTAMP.txt"
LOG_FILE="$LOGS_DIR/cluster_test_log_$TIMESTAMP.txt"
REPORT_FILE="$REPORTS_DIR/cluster_test_report_$TIMESTAMP.txt"

# 清理函数
cleanup() {
    print_section "清理环境"
    if [ -f "$STOP_SCRIPT" ]; then
        echo "停止服务器..."
        bash "$STOP_SCRIPT"
    fi
}

# 设置退出时清理
trap cleanup EXIT

print_header "KV系统集群测试" "测试时间: $(date)"

# 清空结果文件
> "$RESULT_FILE"
> "$LOG_FILE"

# 启动服务器
print_section "启动KV服务器"
bash "$CLEAN_DATA_SCRIPT"
bash "$START_SCRIPT"
sleep 5

# 检查服务器状态
for port in 8000 8001 8002; do
    if ! lsof -i :$port > /dev/null 2>&1; then
        print_failure "端口 $port 服务器启动失败"
        exit 1
    fi
done
print_success "所有服务器启动成功"

# 等待集群稳定
echo "等待集群稳定..."
sleep 10

# 运行Go测试
print_section "运行集群测试"

# 测试raft包
echo "测试raft包..."
cd "$PROJECT_ROOT"
if go test -v ./pkg/raft/ -timeout 60s 2>&1 | tee -a "$LOG_FILE"; then
    print_success "raft测试通过"
    echo "raft测试通过" >> "$RESULT_FILE"
else
    print_failure "raft测试失败"
    echo "raft测试失败" >> "$RESULT_FILE"
fi

# 测试server包
echo "测试server包..."
if go test -v ./pkg/server/ -timeout 60s 2>&1 | tee -a "$LOG_FILE"; then
    print_success "server测试通过"
    echo "server测试通过" >> "$RESULT_FILE"
else
    print_failure "server测试失败"
    echo "server测试失败" >> "$RESULT_FILE"
fi

# 测试WAL并发功能
echo "测试WAL并发功能..."
if go test -v ./pkg/wal/ -run TestWALConcurrentWrites -timeout 30s 2>&1 | tee -a "$LOG_FILE"; then
    print_success "WAL并发测试通过"
    echo "WAL并发测试通过" >> "$RESULT_FILE"
else
    print_failure "WAL并发测试失败"
    echo "WAL并发测试失败" >> "$RESULT_FILE"
fi

# 生成测试报告
echo "" >> "$RESULT_FILE"
echo "=====================================" >> "$RESULT_FILE"
echo "集群测试汇总 - $(date)" >> "$RESULT_FILE"

# 生成报告文件
cat > "$REPORT_FILE" << EOF
# KV系统集群测试报告

## 测试概览
- **测试时间**: $(date)
- **测试脚本**: cluster_test.sh
- **测试类型**: 集群集成测试

## 测试覆盖范围
- Raft分布式一致性算法
- Server gRPC服务
- WAL并发写入
- 集群协调

## 文件位置
- **详细结果**: $RESULT_FILE
- **详细日志**: $LOG_FILE
- **测试报告**: $REPORT_FILE

## 快速查看
EOF

if grep -q "失败" "$RESULT_FILE"; then
    echo "" >> "$REPORT_FILE"
    echo "## 失败的测试" >> "$REPORT_FILE"
    echo "以下测试失败，详细错误信息请查看日志文件：" >> "$REPORT_FILE"
    echo "" >> "$REPORT_FILE"
    grep "失败" "$RESULT_FILE" >> "$REPORT_FILE"
else
    echo "" >> "$REPORT_FILE"
    echo "## 测试状态" >> "$REPORT_FILE"
    echo "✅ 所有集群测试通过！" >> "$REPORT_FILE"
fi

echo "" >> "$REPORT_FILE"
echo "---" >> "$REPORT_FILE"
echo "*报告生成时间: $(date)*" >> "$REPORT_FILE"

# 终端输出
echo ""
if ! grep -q "失败" "$RESULT_FILE"; then
    print_complete "集群测试完成！所有测试通过"
else
    print_failure "集群测试完成！部分测试失败"
fi
echo -e "📊 详细报告: $REPORT_FILE"
echo -e "📋 详细结果: $RESULT_FILE"
echo -e "📝 详细日志: $LOG_FILE"

if ! grep -q "失败" "$RESULT_FILE"; then
    exit 0
else
    exit 1
fi 