#!/bin/bash

# KV系统集成测试脚本
# 测试组件间的交互、分布式功能和端到端场景

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

# 确保目录存在
mkdir -p "$RESULTS_DIR"

# 生成带时间戳的文件名
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
RESULT_FILE="$RESULTS_DIR/integration_test_results_$TIMESTAMP.txt"

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

# 测试计数器
TOTAL_TESTS=0
PASSED_TESTS=0
FAILED_TESTS=0

print_header "KV系统集成测试" "测试时间: $(date)"

# 清空结果文件
> "$RESULT_FILE"

# 执行测试函数
run_integration_test() {
    local test_name="$1"
    local test_commands="$2"
    local expected_pattern="$3"
    local timeout="${4:-30}"
    
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    
    print_progress "运行集成测试: $test_name"
    
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
        $test_commands
    " 2>&1)
    local exit_code=$?
    
    # 检查结果
    if [ $exit_code -eq 0 ] && echo "$output" | grep -q "$expected_pattern"; then
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
for port in 9000 9001 9002; do
    if ! lsof -i :$port > /dev/null 2>&1; then
        echo -e "${RED}✗ 端口 $port 服务器启动失败${NC}"
        exit 1
    fi
done
print_success "所有服务器启动成功"

# 1. 基本操作测试
run_integration_test "基本操作测试" "
    echo 'PUT basic_test test_value' | kv_client
    sleep 0.5
    echo 'GET basic_test' | kv_client
" "test_value"

# 2. 分布式一致性测试
run_integration_test "分布式一致性测试" "
    echo 'PUT consistency_test consistency_value' | kv_client
    sleep 0.5
    echo 'GET consistency_test' | kv_client
    echo 'GET consistency_test' | kv_client
    echo 'GET consistency_test' | kv_client
" "consistency_value"

# 3. 删除操作测试
run_integration_test "删除操作测试" "
    echo 'PUT delete_test delete_value' | kv_client
    sleep 0.5
    echo 'GET delete_test' | kv_client
    echo 'DEL delete_test' | kv_client
    sleep 0.5
    echo 'GET delete_test' | kv_client
" "不存在"

# 4. TTL过期测试
run_integration_test "TTL过期测试" "
    echo 'PUTTTL ttl_test ttl_value 2' | kv_client
    sleep 0.5
    echo 'GET ttl_test' | kv_client
    sleep 3
    echo 'GET ttl_test' | kv_client
" "不存在"

# 5. 并发客户端测试
run_integration_test "并发客户端测试" "
    for i in {1..5}; do
        echo \"PUT concurrent_\$i value_\$i\" | kv_client &
    done
    wait
    sleep 0.5
    echo 'GET concurrent_3' | kv_client
" "value_3"

# 6. 大规模数据测试
run_integration_test "大规模数据测试" "
    for i in {1..20}; do
        echo \"PUT bulk_\$i bulk_value_\$i\" | kv_client
    done
    sleep 1
    echo 'GET bulk_10' | kv_client
" "bulk_value_10"

# 7. 事务一致性测试
run_integration_test "事务一致性测试" "
    echo 'PUT txn_key1 value1' | kv_client
    echo 'PUT txn_key2 value2' | kv_client
    sleep 0.5
    echo 'TXN IF EXISTS txn_key1 THEN PUT txn_key1 new_value1 ELSE PUT txn_key1 default' | kv_client
    sleep 0.5
    echo 'GET txn_key1' | kv_client
" "new_value1"

# 8. MVCC版本控制测试
run_integration_test "MVCC版本控制测试" "
    echo 'PUT mvcc_key initial' | kv_client
    sleep 0.5
    echo 'PUT mvcc_key version1' | kv_client
    sleep 0.5
    echo 'PUT mvcc_key version2' | kv_client
    sleep 0.5
    echo 'HISTORY mvcc_key' | kv_client
" "共"

# 9. 范围查询测试
run_integration_test "范围查询测试" "
    echo 'PUT range_a value_a' | kv_client
    echo 'PUT range_b value_b' | kv_client
    echo 'PUT range_c value_c' | kv_client
    echo 'PUT range_d value_d' | kv_client
    sleep 0.5
    echo 'RANGE range_b range_d' | kv_client
" "range_b"

# 10. 错误处理测试
run_integration_test "错误处理测试" "
    echo 'GET nonexistent_key' | kv_client
    echo 'DEL nonexistent_key' | kv_client
    echo 'PUTTTL ttl_key value -1' | kv_client
" "不存在"

# 11. 数据持久化测试
run_integration_test "数据持久化测试" "
    echo 'PUT persist_test persist_value' | kv_client
    sleep 0.5
    echo 'PUT persist_test2 persist_value2' | kv_client
    sleep 0.5
    # 重启所有服务器
    bash $STOP_SCRIPT
    sleep 1
    bash $START_SCRIPT
    sleep 3
    echo 'GET persist_test' | kv_client
    echo 'GET persist_test2' | kv_client
" "persist_value"

# 12. 性能基准测试
run_integration_test "性能基准测试" "
    start_time=\$(date +%s.%N)
    for i in {1..100}; do
        echo \"PUT perf_\$i perf_value_\$i\" | kv_client > /dev/null 2>&1
    done
    end_time=\$(date +%s.%N)
    duration=\$(echo \"\$end_time - \$start_time\" | bc)
    echo \"性能测试完成，耗时: \${duration}秒\"
    if (( \$(echo \"\$duration < 30\" | bc -l) )); then
        echo \"性能测试通过\"
    else
        echo \"性能测试失败\"
        exit 1
    fi
" "性能测试通过"

# 生成测试报告
echo "" >> "$RESULT_FILE"
echo "=====================================" >> "$RESULT_FILE"
echo "集成测试汇总 - $(date)" >> "$RESULT_FILE"
echo "总计: $TOTAL_TESTS" >> "$RESULT_FILE"
echo "通过: $PASSED_TESTS" >> "$RESULT_FILE"
echo "失败: $FAILED_TESTS" >> "$RESULT_FILE"
echo "成功率: $(echo "scale=1; $PASSED_TESTS * 100 / $TOTAL_TESTS" | bc)%" >> "$RESULT_FILE"

# 终端输出
echo ""
if [ $FAILED_TESTS -eq 0 ]; then
    echo -e "${GREEN}✅ 集成测试完成！所有 $TOTAL_TESTS 个测试通过${NC}"
else
    echo -e "${RED}❌ 集成测试完成！$FAILED_TESTS/$TOTAL_TESTS 个测试失败${NC}"
fi
echo -e "📋 详细结果: $RESULT_FILE"

if [ $FAILED_TESTS -eq 0 ]; then
    exit 0
else
    exit 1
fi 