#!/bin/bash

# KV系统性能基准测试脚本
# 测试系统的性能指标：吞吐量、延迟、资源使用等

set -e

# 脚本路径
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
START_SCRIPT="$PROJECT_ROOT/scripts/start_servers.sh"
STOP_SCRIPT="$PROJECT_ROOT/scripts/stop_servers.sh"
CLEAN_DATA_SCRIPT="$PROJECT_ROOT/scripts/clean_data.sh"

# 导入输出格式工具
source "$(dirname "$0")/output_formatter.sh"

# 预编译客户端，避免每轮 go run 的编译/加载开销
BIN_DIR="$PROJECT_ROOT/bin"
BIN_CLI="$BIN_DIR/client"
mkdir -p "$BIN_DIR"
if [ ! -x "$BIN_CLI" ]; then
    print_progress "编译客户端二进制: $BIN_CLI"
    (cd "$PROJECT_ROOT" && go build -o "$BIN_CLI" ./cmd/client)
fi

# 客户端调用函数 - 使用已编译二进制
kv_client() {
    cd "$PROJECT_ROOT" && "$BIN_CLI"
}

# 运行一批命令（单次会话）
kv_client_batch() {
    local commands="$1"
    cd "$PROJECT_ROOT" && echo -e "$commands" | "$BIN_CLI" > /dev/null 2>&1
}

# 测试输出目录
TEST_DIR="$PROJECT_ROOT/test"
RESULTS_DIR="$TEST_DIR/results"

# 确保目录存在
mkdir -p "$RESULTS_DIR"

# 生成带时间戳的文件名
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
RESULT_FILE="$RESULTS_DIR/benchmark_test_results_$TIMESTAMP.txt"

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

# 执行基准测试函数
run_benchmark() {
    local test_name="$1"
    local test_command="$2"
    local expected_throughput="$3"
    local timeout="${4:-60}"
    local iterations="${5:-5}"
    
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    
    print_progress "运行基准测试: $test_name (${iterations}次迭代)"
    
    # 记录开始时间
    local start_time=$(date +%s.%N)
    
    # 存储多次迭代的结果
    local throughputs=()
    local latencies=()
    local durations=()
    
    # 执行多次迭代
    for i in $(seq 1 $iterations); do
        # 执行测试命令
        local output
        output=$(timeout "$timeout"s bash -c "
            PROJECT_ROOT='$PROJECT_ROOT'
            BIN_CLI='$BIN_CLI'
            kv_client_batch() { cd \"$PROJECT_ROOT\" && echo -e \"$1\" | \"$BIN_CLI\" > /dev/null 2>&1; }
            $test_command
        " 2>&1)
        local exit_code=$?
        
        # 解析性能数据
        if [ $exit_code -eq 0 ]; then
            local throughput=$(echo "$output" | grep "吞吐量:" | awk '{print $2}')
            local latency=$(echo "$output" | grep "平均延迟:" | awk '{print $2}')
            local duration=$(echo "$output" | grep "耗时:" | awk '{print $2}')
            if [ -n "$throughput" ] && [ -n "$latency" ]; then
                throughputs+=("$throughput")
                latencies+=("$latency")
                durations+=("$duration")
            fi
        fi
        
        [ $i -lt $iterations ] && sleep 0.5
    done
    
    # 记录结束时间
    local end_time=$(date +%s.%N)
    local total_duration=$(echo "$end_time - $start_time" | bc -l)
    
    # 计算统计信息
    if [ ${#throughputs[@]} -gt 0 ]; then
        local avg_throughput=$(echo "${throughputs[@]}" | tr ' ' '\n' | awk '{sum+=$1} END {print sum/NR}')
        local avg_latency=$(echo "${latencies[@]}" | tr ' ' '\n' | awk '{sum+=$1} END {print sum/NR}')
        local avg_duration=$(echo "${durations[@]}" | tr ' ' '\n' | awk '{sum+=$1} END {print sum/NR}')
        local throughput_std=$(echo "${throughputs[@]}" | tr ' ' '\n' | awk '{sum+=$1; sumsq+=$1*$1} END {m=sum/NR; print sqrt(sumsq/NR - (m*m))}')
        local latency_std=$(echo "${latencies[@]}" | tr ' ' '\n' | awk '{sum+=$1; sumsq+=$1*$1} END {m=sum/NR; print sqrt(sumsq/NR - (m*m))}')
        local latency_percentiles=$(calculate_percentiles "${latencies[@]}")
        read p0 p50 p90 p95 p99 <<< "$latency_percentiles"

        local success=true
        if [ -n "$expected_throughput" ]; then
            local throughput_ratio=$(echo "scale=2; $avg_throughput / $expected_throughput" | bc -l 2>/dev/null)
            if (( $(echo "$throughput_ratio < 0.8" | bc -l 2>/dev/null) )); then
                success=false
            fi
        fi
        
        if [ "$success" = true ]; then
            print_success "[$TOTAL_TESTS] $test_name"
            echo "[$TOTAL_TESTS] PASS - $test_name" >> "$RESULT_FILE"
            echo "  平均吞吐量: ${avg_throughput} ops/s (±${throughput_std})" >> "$RESULT_FILE"
            echo "  平均延迟: ${avg_latency} ms (±${latency_std})" >> "$RESULT_FILE"
            echo "  延迟百分位: P50=${p50}ms, P90=${p90}ms, P95=${p95}ms, P99=${p99}ms" >> "$RESULT_FILE"
            echo "  迭代次数: ${#throughputs[@]}" >> "$RESULT_FILE"
            PASSED_TESTS=$((PASSED_TESTS + 1))
        else
            print_failure "[$TOTAL_TESTS] $test_name"
            echo "[$TOTAL_TESTS] FAIL - $test_name" >> "$RESULT_FILE"
            echo "  吞吐量不达标: ${avg_throughput} ops/s < ${expected_throughput} ops/s" >> "$RESULT_FILE"
            FAILED_TESTS=$((FAILED_TESTS + 1))
        fi
    else
        print_failure "[$TOTAL_TESTS] $test_name"
        echo "[$TOTAL_TESTS] FAIL - $test_name (无法解析性能数据)" >> "$RESULT_FILE"
        FAILED_TESTS=$((FAILED_TESTS + 1))
    fi
}

# 计算百分位数
calculate_percentiles() {
    local values=("$@")
    local count=${#values[@]}
    
    if [ $count -eq 0 ]; then
        echo "0 0 0 0 0"
        return
    fi
    
    # 排序
    IFS=$'\n' sorted=($(sort -n <<<"${values[*]}"))
    unset IFS
    
    # 计算百分位数
    local p50_idx=$((count * 50 / 100))
    local p90_idx=$((count * 90 / 100))
    local p95_idx=$((count * 95 / 100))
    local p99_idx=$((count * 99 / 100))
    
    echo "${sorted[0]} ${sorted[$p50_idx]} ${sorted[$p90_idx]} ${sorted[$p95_idx]} ${sorted[$p99_idx]}"
}

print_header "KV系统性能基准测试" "测试时间: $(date)"

# 清空结果文件
> "$RESULT_FILE"

# 启动服务器
print_section "启动KV服务器"
bash "$CLEAN_DATA_SCRIPT"
bash "$START_SCRIPT"
sleep 3

# 检查服务器状态
for port in 9000 9001 9002; do
    if ! lsof -i :$port > /dev/null 2>&1; then
        print_failure "端口 $port 服务器启动失败"
        exit 1
    fi
done
print_success "所有服务器启动成功"

# 1. PUT操作性能测试：加大样本量，迭代次数
run_benchmark "PUT操作性能测试" "
    operations=0
    cmds=\"\"
    for i in {1..5000}; do cmds+=\"PUT bench_put_\$i value_\$i\\n\"; done
    start_time=\$(date +%s.%N)
    kv_client_batch \"\$cmds\"
    end_time=\$(date +%s.%N)
    operations=5000
    duration=\$(echo \"\$end_time - \$start_time\" | bc -l)
    throughput=\$(echo \"scale=2; \$operations / \$duration\" | bc -l)
    latency=\$(echo \"scale=4; \$duration * 1000 / \$operations\" | bc -l)
    echo \"操作数: \$operations\"; echo \"耗时: \${duration}秒\"; echo \"吞吐量: \${throughput} ops/s\"; echo \"平均延迟: \${latency} ms\"
" "800" "90" "5"

# 2. GET操作性能测试
run_benchmark "GET操作性能测试" "
    operations=0
    cmds=\"\"
    for i in {1..5000}; do cmds+=\"GET bench_put_\$i\\n\"; done
    start_time=\$(date +%s.%N)
    kv_client_batch \"\$cmds\"
    end_time=\$(date +%s.%N)
    operations=5000
    duration=\$(echo \"\$end_time - \$start_time\" | bc -l)
    throughput=\$(echo \"scale=2; \$operations / \$duration\" | bc -l)
    latency=\$(echo \"scale=4; \$duration * 1000 / \$operations\" | bc -l)
    echo \"操作数: \$operations\"; echo \"耗时: \${duration}秒\"; echo \"吞吐量: \${throughput} ops/s\"; echo \"平均延迟: \${latency} ms\"
" "900" "90" "5"

# 3. 事务性能测试
run_benchmark "事务性能测试" "
    operations=0
    cmds=\"\"
    for i in {1..2000}; do cmds+=\"TXN PUT txn_key_\$i value_\$i PUT txn_key2_\$i value2_\$i\\n\"; done
    start_time=\$(date +%s.%N)
    kv_client_batch \"\$cmds\"
    end_time=\$(date +%s.%N)
    operations=2000
    duration=\$(echo \"\$end_time - \$start_time\" | bc -l)
    throughput=\$(echo \"scale=2; \$operations / \$duration\" | bc -l)
    latency=\$(echo \"scale=4; \$duration * 1000 / \$operations\" | bc -l)
    echo \"操作数: \$operations\"; echo \"耗时: \${duration}秒\"; echo \"吞吐量: \${throughput} ops/s\"; echo \"平均延迟: \${latency} ms\"
" "400" "120" "5"

# 4. 范围查询性能测试
run_benchmark "范围查询性能测试" "
    operations=0
    cmds=\"\"
    for i in {1..1000}; do cmds+=\"RANGE bench_put_1 bench_put_500\\n\"; done
    start_time=\$(date +%s.%N)
    kv_client_batch \"\$cmds\"
    end_time=\$(date +%s.%N)
    operations=1000
    duration=\$(echo \"\$end_time - \$start_time\" | bc -l)
    throughput=\$(echo \"scale=2; \$operations / \$duration\" | bc -l)
    latency=\$(echo \"scale=4; \$duration * 1000 / \$operations\" | bc -l)
    echo \"操作数: \$operations\"; echo \"耗时: \${duration}秒\"; echo \"吞吐量: \${throughput} ops/s\"; echo \"平均延迟: \${latency} ms\"
" "150" "120" "5"

# 5. TTL操作性能测试
run_benchmark "TTL操作性能测试" "
    operations=0
    cmds=\"\"
    for i in {1..2000}; do cmds+=\"PUTTTL ttl_bench_\$i value_\$i 60\\n\"; done
    start_time=\$(date +%s.%N)
    kv_client_batch \"\$cmds\"
    end_time=\$(date +%s.%N)
    operations=2000
    duration=\$(echo \"\$end_time - \$start_time\" | bc -l)
    throughput=\$(echo \"scale=2; \$operations / \$duration\" | bc -l)
    latency=\$(echo \"scale=4; \$duration * 1000 / \$operations\" | bc -l)
    echo \"操作数: \$operations\"; echo \"耗时: \${duration}秒\"; echo \"吞吐量: \${throughput} ops/s\"; echo \"平均延迟: \${latency} ms\"
" "350" "120" "5"

# 6. 并发性能测试（仍为批量，后续可提供并发 loadgen）
run_benchmark "并发性能测试" "
    operations=0
    cmds=\"\"
    for i in {1..2000}; do cmds+=\"PUT concurrent_key_\$i value_\$i\\n\"; done
    start_time=\$(date +%s.%N)
    kv_client_batch \"\$cmds\"
    end_time=\$(date +%s.%N)
    operations=2000
    duration=\$(echo \"\$end_time - \$start_time\" | bc -l)
    throughput=\$(echo \"scale=2; \$operations / \$duration\" | bc -l)
    latency=\$(echo \"scale=4; \$duration * 1000 / \$operations\" | bc -l)
    echo \"操作数: \$operations\"; echo \"耗时: \${duration}秒\"; echo \"吞吐量: \${throughput} ops/s\"; echo \"平均延迟: \${latency} ms\"
" "350" "120" "5"

# 7. 大数据量性能测试
run_benchmark "大数据量性能测试" "
    operations=0
    cmds=\"\"
    for i in {1..500}; do large_value=\$(printf '%.2000d' \$i); cmds+=\"PUT large_key_\$i \$large_value\\n\"; done
    start_time=\$(date +%s.%N)
    kv_client_batch \"\$cmds\"
    end_time=\$(date +%s.%N)
    operations=500
    duration=\$(echo \"\$end_time - \$start_time\" | bc -l)
    throughput=\$(echo \"scale=2; \$operations / \$duration\" | bc -l)
    latency=\$(echo \"scale=4; \$duration * 1000 / \$operations\" | bc -l)
    echo \"操作数: \$operations\"; echo \"耗时: \${duration}秒\"; echo \"吞吐量: \${throughput} ops/s\"; echo \"平均延迟: \${latency} ms\"
" "80" "180" "2"

# 汇总与报告
{
    echo ""; echo "====================================="; echo "性能基准测试汇总 - $(date)"; echo "总计: $TOTAL_TESTS"; echo "通过: $PASSED_TESTS"; echo "失败: $FAILED_TESTS"; echo "成功率: $(echo "scale=1; $PASSED_TESTS * 100 / $TOTAL_TESTS" | bc)%"
} >> "$RESULT_FILE"

# 终端输出
echo ""
if [ $FAILED_TESTS -eq 0 ]; then
    print_complete "性能基准测试完成！所有 $TOTAL_TESTS 个测试通过"
else
    print_failure "性能基准测试完成！$FAILED_TESTS/$TOTAL_TESTS 个测试失败"
fi
echo -e "📋 详细结果: $RESULT_FILE"

[ $FAILED_TESTS -eq 0 ] && exit 0 || exit 1 