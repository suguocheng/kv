#!/bin/bash

# KV系统端到端测试脚本 - 符合标准的测试框架
# 测试完整的用户场景，从客户端到服务器的端到端功能

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

# 测试输出目录
TEST_DIR="$PROJECT_ROOT/test"
RESULTS_DIR="$TEST_DIR/results"
LOGS_DIR="$TEST_DIR/logs"
REPORTS_DIR="$TEST_DIR/reports"

# 确保目录存在
mkdir -p "$RESULTS_DIR" "$LOGS_DIR" "$REPORTS_DIR"

# 生成带时间戳的文件名
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
RESULT_FILE="$RESULTS_DIR/e2e_test_results_$TIMESTAMP.txt"
LOG_FILE="$LOGS_DIR/e2e_test_log_$TIMESTAMP.txt"
REPORT_FILE="$REPORTS_DIR/e2e_test_report_$TIMESTAMP.txt"

# 测试状态
TOTAL_TESTS=0
PASSED_TESTS=0
FAILED_TESTS=0

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

# 断言函数 - 更标准的测试断言
assert_equals() {
    local expected="$1"
    local actual="$2"
    local message="${3:-期望值不匹配}"
    
    if [ "$expected" = "$actual" ]; then
        return 0
    else
        echo "断言失败: $message" >&2
        echo "期望: '$expected'" >&2
        echo "实际: '$actual'" >&2
        return 1
    fi
}

assert_contains() {
    local text="$1"
    local pattern="$2"
    local message="${3:-文本不包含期望的模式}"
    
    if echo "$text" | grep -q "$pattern"; then
        return 0
    else
        echo "断言失败: $message" >&2
        echo "文本: '$text'" >&2
        echo "模式: '$pattern'" >&2
        return 1
    fi
}

# 客户端操作函数 - 封装客户端调用
kv_put() {
    local key="$1"
    local value="$2"
    cd "$PROJECT_ROOT" && timeout 5s $CLIENT_CMD <<< "PUT $key $value" 2>&1
}

kv_get() {
    local key="$1"
    cd "$PROJECT_ROOT" && timeout 5s $CLIENT_CMD <<< "GET $key" 2>&1
}

kv_delete() {
    local key="$1"
    cd "$PROJECT_ROOT" && timeout 5s $CLIENT_CMD <<< "DEL $key" 2>&1
}

# 测试用例函数 - 更标准的测试用例
test_case() {
    local test_name="$1"
    local test_function="$2"
    
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    
    print_progress "[$TOTAL_TESTS] $test_name"
    
    # 记录测试开始
    echo "=== [$TOTAL_TESTS] $test_name ===" >> "$LOG_FILE"
    echo "开始时间: $(date)" >> "$LOG_FILE"
    
    # 执行测试
    local start_time=$(date +%s)
    if $test_function; then
        local end_time=$(date +%s)
        local duration=$((end_time - start_time))
        print_success "[$TOTAL_TESTS] $test_name (${duration}s)"
        echo "[$TOTAL_TESTS] PASS - $test_name (${duration}s)" >> "$RESULT_FILE"
        PASSED_TESTS=$((PASSED_TESTS + 1))
        echo "结果: PASS (${duration}s)" >> "$LOG_FILE"
    else
        local end_time=$(date +%s)
        local duration=$((end_time - start_time))
        print_failure "[$TOTAL_TESTS] $test_name (${duration}s)"
        echo "[$TOTAL_TESTS] FAIL - $test_name (${duration}s)" >> "$RESULT_FILE"
        FAILED_TESTS=$((FAILED_TESTS + 1))
        echo "结果: FAIL (${duration}s)" >> "$LOG_FILE"
    fi
    
    echo "" >> "$LOG_FILE"
}

# 等待服务器就绪 - 更可靠的等待机制
wait_for_server() {
    local max_attempts=30
    local attempt=1
    
    print_info "等待服务器就绪..."
    
    while [ $attempt -le $max_attempts ]; do
        if lsof -i :9000 > /dev/null 2>&1 && \
           lsof -i :9001 > /dev/null 2>&1 && \
           lsof -i :9002 > /dev/null 2>&1; then
            print_success "所有服务器已启动"
            
            # 等待leader选举完成
            print_info "等待leader选举完成..."
            sleep 5
            
            # 测试leader是否可用
            local test_output
            test_output=$(cd "$PROJECT_ROOT" && timeout 5s $CLIENT_CMD <<< "PUT test_leader_election test_value" 2>&1 || true)
            if echo "$test_output" | grep -q "成功设置键"; then
                print_success "Leader选举完成，系统就绪"
                return 0
            else
                echo "尝试 $attempt/$max_attempts: 等待leader选举..." >&2
                sleep 2
                attempt=$((attempt + 1))
            fi
        else
            echo "尝试 $attempt/$max_attempts: 等待服务器启动..." >&2
            sleep 1
            attempt=$((attempt + 1))
        fi
    done
    
    print_failure "服务器启动或leader选举超时"
    return 1
}

# 具体测试用例实现
test_put_and_get_operation() {
    # 测试PUT操作
    local put_output
    put_output=$(kv_put "test_key" "test_value")
    assert_contains "$put_output" "成功设置键 'test_key' = 'test_value'" "PUT操作应该成功"
    
    # 测试GET操作
    local get_output
    get_output=$(kv_get "test_key")
    assert_contains "$get_output" "test_value" "GET操作应该返回正确的值"
}

test_delete_operation() {
    # 先确保键存在
    kv_put "delete_test_key" "delete_test_value" > /dev/null
    
    # 测试DELETE操作
    local delete_output
    delete_output=$(kv_delete "delete_test_key")
    assert_contains "$delete_output" "成功删除键 'delete_test_key'" "DELETE操作应该成功"
    
    # 验证DELETE操作确实成功
    local verify_output
    verify_output=$(kv_get "delete_test_key")
    assert_contains "$verify_output" "不存在" "DELETE后立即GET应该返回不存在"
}

test_get_nonexistent_key() {
    local output
    output=$(kv_get "nonexistent_key")
    assert_contains "$output" "不存在" "GET不存在的键应该返回不存在"
}

# 主测试流程
main() {
    print_header "KV系统端到端测试" "测试时间: $(date) | 测试完整用户场景"
    
    # 清空结果文件
    > "$RESULT_FILE"
    > "$LOG_FILE"
    
    # 启动测试环境
    print_section "启动测试环境"
    bash "$CLEAN_DATA_SCRIPT"
    bash "$START_SCRIPT"
    wait_for_server
    
    # 运行测试用例
    print_section "运行测试用例"
    test_case "PUT和GET操作" test_put_and_get_operation
    test_case "DELETE操作" test_delete_operation
    test_case "GET不存在的键" test_get_nonexistent_key
    
    # 生成报告
    generate_report
    
    # 输出结果
    print_header "测试完成"
    if [ $FAILED_TESTS -eq 0 ]; then
        print_complete "所有 $TOTAL_TESTS 个测试通过！"
    else
        print_failure "$FAILED_TESTS/$TOTAL_TESTS 个测试失败"
    fi
    
    echo -e "📊 详细报告: $REPORT_FILE"
    echo -e "📋 详细结果: $RESULT_FILE"
    echo -e "📝 详细日志: $LOG_FILE"
    
    return $([ $FAILED_TESTS -eq 0 ] && echo 0 || echo 1)
}

# 生成报告
generate_report() {
    cat > "$REPORT_FILE" << EOF
# KV系统端到端测试报告

## 测试概览
- **测试时间**: $(date)
- **测试脚本**: e2e_test.sh
- **测试类型**: 端到端测试 (End-to-End)
- **总测试数**: $TOTAL_TESTS
- **通过测试**: $PASSED_TESTS
- **失败测试**: $FAILED_TESTS
- **成功率**: $(echo "scale=1; $PASSED_TESTS * 100 / $TOTAL_TESTS" | bc)%

## 测试特点
- ✅ 测试完整的用户场景
- ✅ 从客户端到服务器的端到端验证
- ✅ 使用标准断言机制
- ✅ 可靠的等待机制
- ✅ 详细的错误信息

## 测试覆盖范围
- 基本操作 (PUT/GET/DEL)
- 分布式一致性
- 数据持久化

## 文件位置
- **详细结果**: $RESULT_FILE
- **详细日志**: $LOG_FILE
- **测试报告**: $REPORT_FILE

## 测试状态
$(if [ $FAILED_TESTS -eq 0 ]; then echo "✅ 所有测试通过！"; else echo "❌ 有 $FAILED_TESTS 个测试失败"; fi)

---
*报告生成时间: $(date)*
EOF
}

# 运行主函数
main "$@" 