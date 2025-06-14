#!/bin/zsh

# 查找并杀掉所有 main.go 节点进程
pids=($(ps aux | grep '../cmd/server/main.go' | grep -v grep | awk '{print $2}'))

if [[ ${#pids[@]} -eq 0 ]]; then
  echo "No server processes found."
else
  echo "Killing server processes: $pids"
  kill $pids
fi