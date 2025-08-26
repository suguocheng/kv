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

# 预编译客户端，避免 go run 带来的不稳定与开销
BIN_DIR="$PROJECT_ROOT/bin"
BIN_CLI="$BIN_DIR/client"
mkdir -p "$BIN_DIR"
if [ ! -x "$BIN_CLI" ]; then
    print_progress "编译客户端二进制: $BIN_CLI"
    (cd "$PROJECT_ROOT" && go build -o "$BIN_CLI" ./cmd/client)
fi

# 测试输出目录
TEST_DIR="$PROJECT_ROOT/test"
RESULTS_DIR="$TEST_DIR/results"

# 确保目录存在
mkdir -p "$RESULTS_DIR"

# 生成带时间戳的文件名
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
RESULT_FILE="$RESULTS_DIR/e2e_test_results_$TIMESTAMP.txt"

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

# 执行客户端命令（带重试）
kv_exec() {
    local cmd_line="$1"
    local max_attempts="${2:-5}"
    local delay_sec="${3:-1}"
    local attempt=1
    local output=""
    while [ $attempt -le $max_attempts ]; do
        output=$(cd "$PROJECT_ROOT" && timeout 5s "$BIN_CLI" <<< "$cmd_line" 2>&1 || true)
        # 若连接池初始化失败或没有任何连接，进行重试
        if echo "$output" | grep -qi "failed to create any initial connections\|failed to add server"; then
            sleep "$delay_sec"
            attempt=$((attempt + 1))
            continue
        fi
        break
    done
    echo "$output"
}

# 客户端操作函数 - 封装客户端调用
kv_put() {
    local key="$1"
    local value="$2"
    kv_exec "PUT $key $value"
}

kv_get() {
    local key="$1"
    kv_exec "GET $key"
}

kv_delete() {
    local key="$1"
    kv_exec "DEL $key"
}

# 测试用例函数 - 更标准的测试用例
test_case() {
    local test_name="$1"
    local test_function="$2"
    
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    
    print_progress "[$TOTAL_TESTS] $test_name"
    
    # 执行测试
    local start_time=$(date +%s)
    if $test_function; then
        local end_time=$(date +%s)
        local duration=$((end_time - start_time))
        print_success "[$TOTAL_TESTS] $test_name (${duration}s)"
        echo "[$TOTAL_TESTS] PASS - $test_name (${duration}s)" >> "$RESULT_FILE"
        PASSED_TESTS=$((PASSED_TESTS + 1))
    else
        local end_time=$(date +%s)
        local duration=$((end_time - start_time))
        print_failure "[$TOTAL_TESTS] $test_name (${duration}s)"
        echo "[$TOTAL_TESTS] FAIL - $test_name (${duration}s)" >> "$RESULT_FILE"
        FAILED_TESTS=$((FAILED_TESTS + 1))
    fi
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
            sleep 3
            
            # 测试leader是否可用（带重试）
            local test_output
            test_output=$(kv_exec "PUT test_leader_election test_value" 8 1)
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
    
    # 测试GET操作（对可能的连接重建给予短暂缓冲）
    sleep 0.2
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
    
    # 验证DELETE操作确实成功（短暂重试，避免瞬时时序抖动）
    local verify_output=""
    local found_absent=false
    for attempt in {1..10}; do
        verify_output=$(kv_get "delete_test_key")
        if echo "$verify_output" | grep -q "不存在"; then
            found_absent=true
            break
        fi
        sleep 0.1
    done
    if [ "$found_absent" != true ]; then
        # 最后一版输出进入断言，便于错误信息呈现
        assert_contains "$verify_output" "不存在" "DELETE后GET应该返回不存在（已重试）"
    fi
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
    
    # 输出结果
    print_header "测试完成"
    if [ $FAILED_TESTS -eq 0 ]; then
        print_complete "所有 $TOTAL_TESTS 个测试通过！"
    else
        print_failure "$FAILED_TESTS/$TOTAL_TESTS 个测试失败"
    fi
    
    echo -e "📋 详细结果: $RESULT_FILE"
    
    return $([ $FAILED_TESTS -eq 0 ] && echo 0 || echo 1)
}

# 运行主函数
main "$@" 