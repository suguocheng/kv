#!/bin/bash

# KV系统故障模拟脚本
# 用于模拟各种分布式环境中的故障场景

set -e

# 脚本路径
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# 导入输出格式工具
source "$(dirname "$0")/output_formatter.sh"

# 节点配置
NODE_PORTS=(9000 9001 9002)
NODE_NAMES=("node0" "node1" "node2")

# 故障类型
FAULT_TYPES=("node_failure" "network_partition" "slow_network" "restart")

# 获取节点PID
get_node_pid() {
    local node_id="$1"
    local port="${NODE_PORTS[$node_id]}"
    lsof -ti :$port 2>/dev/null || echo ""
}

# 检查节点状态
check_node_status() {
    local node_id="$1"
    local port="${NODE_PORTS[$node_id]}"
    if lsof -i :$port > /dev/null 2>&1; then
        echo "在线"
        return 0
    else
        echo "离线"
        return 1
    fi
}

# 显示集群状态
show_cluster_status() {
    print_section "集群状态"
    for i in {0..2}; do
        local status=$(check_node_status $i)
        local pid=$(get_node_pid $i)
        echo "  ${NODE_NAMES[$i]} (端口 ${NODE_PORTS[$i]}): $status (PID: ${pid:-N/A})"
    done
}

# 模拟节点故障
simulate_node_failure() {
    local node_id="$1"
    local duration="${2:-30}"
    
    if [ $node_id -lt 0 ] || [ $node_id -gt 2 ]; then
        print_failure "无效的节点ID: $node_id (应该是 0-2)"
        return 1
    fi
    
    local port="${NODE_PORTS[$node_id]}"
    local node_name="${NODE_NAMES[$node_id]}"
    
    print_warning "模拟 $node_name 故障 (端口 $port, 持续 ${duration}秒)"
    
    # 获取并杀死进程
    local pids=$(lsof -ti :$port 2>/dev/null || true)
    if [ -n "$pids" ]; then
        echo "  杀死进程: $pids"
        echo "$pids" | xargs kill -9 2>/dev/null || true
        sleep 2
        
        # 等待指定时间后恢复
        print_info "等待 ${duration}秒后恢复节点..."
        sleep $duration
        
        # 恢复节点
        print_info "恢复 $node_name"
        cd "$PROJECT_ROOT"
        nohup bin/server $node_id > "log/test${node_id}.log" 2>&1 &
        sleep 3
        
        if check_node_status $node_id > /dev/null; then
            print_success "$node_name 恢复成功"
        else
            print_failure "$node_name 恢复失败"
        fi
    else
        print_warning "$node_name 已经离线"
    fi
}

# 模拟网络分区
simulate_network_partition() {
    local partition1="$1"  # 节点ID列表，用逗号分隔
    local partition2="$2"  # 节点ID列表，用逗号分隔
    local duration="${3:-30}"
    
    print_warning "模拟网络分区: [$partition1] vs [$partition2] (持续 ${duration}秒)"
    
    # 这里可以实现更复杂的网络分区模拟
    # 目前我们使用简单的节点故障来模拟
    print_info "注意: 当前实现使用节点故障来模拟网络分区"
    
    # 故障第一组中的第一个节点
    local first_node=$(echo "$partition1" | cut -d',' -f1)
    if [ -n "$first_node" ]; then
        simulate_node_failure $first_node $duration
    fi
}

# 模拟慢网络
simulate_slow_network() {
    local node_id="$1"
    local delay="${2:-1000}"  # 毫秒
    local duration="${3:-30}"
    
    print_warning "模拟 $node_name 慢网络 (延迟 ${delay}ms, 持续 ${duration}秒)"
    print_info "注意: 当前实现不支持网络延迟模拟，使用节点重启代替"
    
    # 重启节点来模拟网络问题
    local port="${NODE_PORTS[$node_id]}"
    local pids=$(lsof -ti :$port 2>/dev/null || true)
    if [ -n "$pids" ]; then
        echo "$pids" | xargs kill -9 2>/dev/null || true
        sleep 2
        
        cd "$PROJECT_ROOT"
        nohup bin/server $node_id > "log/test${node_id}.log" 2>&1 &
        sleep 3
    fi
}

# 模拟节点重启
simulate_node_restart() {
    local node_id="$1"
    local restart_delay="${2:-5}"
    
    local node_name="${NODE_NAMES[$node_id]}"
    print_warning "模拟 $node_name 重启 (延迟 ${restart_delay}秒)"
    
    # 停止节点
    local port="${NODE_PORTS[$node_id]}"
    local pids=$(lsof -ti :$port 2>/dev/null || true)
    if [ -n "$pids" ]; then
        echo "  停止 $node_name"
        echo "$pids" | xargs kill -9 2>/dev/null || true
        sleep $restart_delay
        
        # 重启节点
        echo "  重启 $node_name"
        cd "$PROJECT_ROOT"
        nohup bin/server $node_id > "log/test${node_id}.log" 2>&1 &
        sleep 3
        
        if check_node_status $node_id > /dev/null; then
            print_success "$node_name 重启成功"
        else
            print_failure "$node_name 重启失败"
        fi
    else
        print_warning "$node_name 已经离线"
    fi
}

# 随机故障模拟
simulate_random_faults() {
    local duration="${1:-60}"
    local fault_interval="${2:-10}"
    
    print_warning "开始随机故障模拟 (总时长: ${duration}秒, 故障间隔: ${fault_interval}秒)"
    
    local start_time=$(date +%s)
    local end_time=$((start_time + duration))
    
    while [ $(date +%s) -lt $end_time ]; do
        # 随机选择故障类型和节点
        local fault_type=${FAULT_TYPES[$((RANDOM % ${#FAULT_TYPES[@]}))]}
        local node_id=$((RANDOM % 3))
        
        case $fault_type in
            "node_failure")
                print_info "随机故障: 节点 $node_id 故障"
                simulate_node_failure $node_id 5
                ;;
            "restart")
                print_info "随机故障: 节点 $node_id 重启"
                simulate_node_restart $node_id 3
                ;;
            *)
                print_info "随机故障: $fault_type (节点 $node_id)"
                simulate_node_failure $node_id 5
                ;;
        esac
        
        sleep $fault_interval
    done
    
    print_success "随机故障模拟完成"
}

# 主函数
main() {
    local command="$1"
    shift
    
    case $command in
        "status")
            show_cluster_status
            ;;
        "fail")
            local node_id="$1"
            local duration="${2:-30}"
            simulate_node_failure $node_id $duration
            ;;
        "partition")
            local partition1="$1"
            local partition2="$2"
            local duration="${3:-30}"
            simulate_network_partition "$partition1" "$partition2" $duration
            ;;
        "slow")
            local node_id="$1"
            local delay="${2:-1000}"
            local duration="${3:-30}"
            simulate_slow_network $node_id $delay $duration
            ;;
        "restart")
            local node_id="$1"
            local delay="${2:-5}"
            simulate_node_restart $node_id $delay
            ;;
        "random")
            local duration="${1:-60}"
            local interval="${2:-10}"
            simulate_random_faults $duration $interval
            ;;
        "help"|*)
            echo "用法: $0 <command> [args...]"
            echo ""
            echo "命令:"
            echo "  status                    - 显示集群状态"
            echo "  fail <node_id> [duration] - 模拟节点故障"
            echo "  partition <nodes1> <nodes2> [duration] - 模拟网络分区"
            echo "  slow <node_id> [delay] [duration] - 模拟慢网络"
            echo "  restart <node_id> [delay] - 模拟节点重启"
            echo "  random [duration] [interval] - 随机故障模拟"
            echo "  help                      - 显示帮助"
            echo ""
            echo "示例:"
            echo "  $0 status"
            echo "  $0 fail 0 30"
            echo "  $0 partition \"0,1\" \"2\" 60"
            echo "  $0 restart 1 5"
            echo "  $0 random 120 15"
            ;;
    esac
}

# 运行主函数
main "$@"