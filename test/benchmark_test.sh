#!/bin/bash

# KV系统性能基准测试脚本
# 测试系统的性能指标：吞吐量、延迟等

set -e

# 脚本路径
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
START_SCRIPT="$PROJECT_ROOT/scripts/start_servers.sh"
STOP_SCRIPT="$PROJECT_ROOT/scripts/stop_servers.sh"
CLEAN_DATA_SCRIPT="$PROJECT_ROOT/scripts/clean_data.sh"

# 导入输出格式工具
source "$(dirname "$0")/output_formatter.sh"

# 客户端命令 - 在项目根目录下运行以确保历史文件路径正确
CLIENT_CMD="go run $PROJECT_ROOT/cmd/client/main.go $PROJECT_ROOT/cmd/client/handlers.go $PROJECT_ROOT/cmd/client/help.go"

# 客户端调用函数 - 确保在正确的目录下运行
kv_client() {
    cd "$PROJECT_ROOT" && $CLIENT_CMD
}

# 测试输出目录
TEST_DIR="$PROJECT_ROOT/test"
RESULTS_DIR="$TEST_DIR/results"
LOGS_DIR="$TEST_DIR/logs"
REPORTS_DIR="$TEST_DIR/reports"

# 确保目录存在
mkdir -p "$RESULTS_DIR" "$LOGS_DIR" "$REPORTS_DIR"

# 生成带时间戳的文件名
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
RESULT_FILE="$RESULTS_DIR/benchmark_test_results_$TIMESTAMP.txt"
LOG_FILE="$LOGS_DIR/benchmark_test_log_$TIMESTAMP.txt"
REPORT_FILE="$REPORTS_DIR/benchmark_test_report_$TIMESTAMP.txt"

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

# 性能测试计数器
TOTAL_TESTS=0
PASSED_TESTS=0
FAILED_TESTS=0

print_header "KV系统性能基准测试" "测试时间: $(date)"

# 清空结果文件
> "$RESULT_FILE"
> "$LOG_FILE"

# 执行基准测试函数
run_benchmark() {
    local test_name="$1"
    local test_command="$2"
    local expected_throughput="$3"
    local timeout="${4:-30}"
    
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    
    print_progress "运行基准测试: $test_name"
    
    # 记录开始时间
    local start_time=$(date +%s.%N)
    
    # 执行测试命令 - 使用当前shell环境以访问kv_client函数
    local output
    output=$(timeout "$timeout"s bash -c "
        # 导入必要的变量和函数
        PROJECT_ROOT='$PROJECT_ROOT'
        CLIENT_CMD='$CLIENT_CMD'
        
        # 重新定义kv_client函数
        kv_client() {
            cd \"\$PROJECT_ROOT\" && \$CLIENT_CMD
        }
        
        # 执行测试命令
        $test_command
    " 2>&1)
    local exit_code=$?
    
    # 记录结束时间
    local end_time=$(date +%s.%N)
    local duration=$(echo "$end_time - $start_time" | bc -l)
    
    # 记录到日志文件
    echo "=== [$TOTAL_TESTS] $test_name ===" >> "$LOG_FILE"
    echo "命令: $test_command" >> "$LOG_FILE"
    echo "输出: $output" >> "$LOG_FILE"
    echo "退出码: $exit_code" >> "$LOG_FILE"
    echo "耗时: ${duration}秒" >> "$LOG_FILE"
    echo "" >> "$LOG_FILE"
    
    # 检查结果
    if [ $exit_code -eq 0 ] && echo "$output" | grep -q "ops/s"; then
        print_success "[$TOTAL_TESTS] $test_name"
        echo "[$TOTAL_TESTS] PASS - $test_name" >> "$RESULT_FILE"
        PASSED_TESTS=$((PASSED_TESTS + 1))
    else
        print_failure "[$TOTAL_TESTS] $test_name"
        echo "[$TOTAL_TESTS] FAIL - $test_name" >> "$RESULT_FILE"
        FAILED_TESTS=$((FAILED_TESTS + 1))
    fi
}

# 启动服务器
print_section "启动KV服务器"
bash "$CLEAN_DATA_SCRIPT"
bash "$START_SCRIPT"
sleep 3

# 检查服务器状态
for port in 8000 8001 8002; do
    if ! lsof -i :$port > /dev/null 2>&1; then
        print_failure "端口 $port 服务器启动失败"
        exit 1
    fi
done
print_success "所有服务器启动成功"

# 1. PUT操作性能测试
run_benchmark "PUT操作性能测试" "
    echo '开始PUT性能测试...'
    start_time=\$(date +%s.%N)
    operations=0
    for i in {1..100}; do
        echo \"PUT bench_put_\$i value_\$i\" | kv_client > /dev/null 2>&1
        operations=\$((operations + 1))
    done
    end_time=\$(date +%s.%N)
    duration=\$(echo \"\$end_time - \$start_time\" | bc -l)
    throughput=\$(echo \"scale=2; \$operations / \$duration\" | bc -l)
    latency=\$(echo \"scale=3; \$duration * 1000 / \$operations\" | bc -l)
    echo \"操作数: \$operations\"
    echo \"耗时: \${duration}秒\"
    echo \"吞吐量: \${throughput} ops/s\"
    echo \"平均延迟: \${latency} ms\"
" "50"

# 2. GET操作性能测试
run_benchmark "GET操作性能测试" "
    echo '开始GET性能测试...'
    start_time=\$(date +%s.%N)
    operations=0
    for i in {1..100}; do
        echo \"GET bench_put_\$i\" | kv_client > /dev/null 2>&1
        operations=\$((operations + 1))
    done
    end_time=\$(date +%s.%N)
    duration=\$(echo \"\$end_time - \$start_time\" | bc -l)
    throughput=\$(echo \"scale=2; \$operations / \$duration\" | bc -l)
    latency=\$(echo \"scale=3; \$duration * 1000 / \$operations\" | bc -l)
    echo \"操作数: \$operations\"
    echo \"耗时: \${duration}秒\"
    echo \"吞吐量: \${throughput} ops/s\"
    echo \"平均延迟: \${latency} ms\"
" "100"

# 3. 事务性能测试
run_benchmark "事务性能测试" "
    echo '开始事务性能测试...'
    start_time=\$(date +%s.%N)
    operations=0
    for i in {1..50}; do
        echo \"TXN PUT txn_key_\$i value_\$i PUT txn_key2_\$i value2_\$i\" | kv_client > /dev/null 2>&1
        operations=\$((operations + 1))
    done
    end_time=\$(date +%s.%N)
    duration=\$(echo \"\$end_time - \$start_time\" | bc -l)
    throughput=\$(echo \"scale=2; \$operations / \$duration\" | bc -l)
    latency=\$(echo \"scale=3; \$duration * 1000 / \$operations\" | bc -l)
    echo \"操作数: \$operations\"
    echo \"耗时: \${duration}秒\"
    echo \"吞吐量: \${throughput} ops/s\"
    echo \"平均延迟: \${latency} ms\"
" "10"

# 4. 范围查询性能测试
run_benchmark "范围查询性能测试" "
    echo '开始范围查询性能测试...'
    start_time=\$(date +%s.%N)
    operations=0
    for i in {1..20}; do
        echo 'RANGE bench_put_1 bench_put_50' | kv_client > /dev/null 2>&1
        operations=\$((operations + 1))
    done
    end_time=\$(date +%s.%N)
    duration=\$(echo \"\$end_time - \$start_time\" | bc -l)
    throughput=\$(echo \"scale=2; \$operations / \$duration\" | bc -l)
    latency=\$(echo \"scale=3; \$duration * 1000 / \$operations\" | bc -l)
    echo \"操作数: \$operations\"
    echo \"耗时: \${duration}秒\"
    echo \"吞吐量: \${throughput} ops/s\"
    echo \"平均延迟: \${latency} ms\"
" "5"

# 5. TTL操作性能测试
run_benchmark "TTL操作性能测试" "
    echo '开始TTL操作性能测试...'
    start_time=\$(date +%s.%N)
    operations=0
    for i in {1..50}; do
        echo \"PUTTTL ttl_bench_\$i value_\$i 60\" | kv_client > /dev/null 2>&1
        operations=\$((operations + 1))
    done
    end_time=\$(date +%s.%N)
    duration=\$(echo \"\$end_time - \$start_time\" | bc -l)
    throughput=\$(echo \"scale=2; \$operations / \$duration\" | bc -l)
    latency=\$(echo \"scale=3; \$duration * 1000 / \$operations\" | bc -l)
    echo \"操作数: \$operations\"
    echo \"耗时: \${duration}秒\"
    echo \"吞吐量: \${throughput} ops/s\"
    echo \"平均延迟: \${latency} ms\"
" "20"

# 生成测试报告
echo "" >> "$RESULT_FILE"
echo "=====================================" >> "$RESULT_FILE"
echo "性能基准测试汇总 - $(date)" >> "$RESULT_FILE"
echo "总计: $TOTAL_TESTS" >> "$RESULT_FILE"
echo "通过: $PASSED_TESTS" >> "$RESULT_FILE"
echo "失败: $FAILED_TESTS" >> "$RESULT_FILE"
echo "成功率: $(echo "scale=1; $PASSED_TESTS * 100 / $TOTAL_TESTS" | bc)%" >> "$RESULT_FILE"

# 生成报告文件
cat > "$REPORT_FILE" << EOF
# KV系统性能基准测试报告

## 测试概览
- **测试时间**: $(date)
- **测试脚本**: benchmark_test.sh
- **总测试数**: $TOTAL_TESTS
- **通过测试**: $PASSED_TESTS
- **失败测试**: $FAILED_TESTS
- **成功率**: $(echo "scale=1; $PASSED_TESTS * 100 / $TOTAL_TESTS" | bc)%

## 测试覆盖范围
- PUT操作性能
- GET操作性能
- 事务性能
- 范围查询性能
- TTL操作性能

## 性能指标
- **吞吐量**: 操作数/秒 (ops/s)
- **延迟**: 平均响应时间 (ms)

## 文件位置
- **详细结果**: $RESULT_FILE
- **详细日志**: $LOG_FILE
- **测试报告**: $REPORT_FILE

## 快速查看
EOF

if [ $FAILED_TESTS -gt 0 ]; then
    echo "" >> "$REPORT_FILE"
    echo "## 失败的测试" >> "$REPORT_FILE"
    echo "以下测试失败，详细错误信息请查看日志文件：" >> "$REPORT_FILE"
    echo "" >> "$REPORT_FILE"
    grep "FAIL" "$RESULT_FILE" >> "$REPORT_FILE"
else
    echo "" >> "$REPORT_FILE"
    echo "## 测试状态" >> "$REPORT_FILE"
    echo "✅ 所有性能基准测试通过！" >> "$REPORT_FILE"
fi

echo "" >> "$REPORT_FILE"
echo "---" >> "$REPORT_FILE"
echo "*报告生成时间: $(date)*" >> "$REPORT_FILE"

# 终端输出
echo ""
if [ $FAILED_TESTS -eq 0 ]; then
    print_complete "性能基准测试完成！所有 $TOTAL_TESTS 个测试通过"
else
    print_failure "性能基准测试完成！$FAILED_TESTS/$TOTAL_TESTS 个测试失败"
fi
echo -e "📊 详细报告: $REPORT_FILE"
echo -e "📋 详细结果: $RESULT_FILE"
echo -e "📝 详细日志: $LOG_FILE"

if [ $FAILED_TESTS -eq 0 ]; then
    exit 0
else
    exit 1
fi 