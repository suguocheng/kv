#!/bin/bash

for port in 8000 8001 8002; do
  pids=$(lsof -ti tcp:$port)
  if [ -n "$pids" ]; then
    echo "关闭端口 $port 的进程: $pids"
    kill $pids
  fi
done

echo "所有端口相关进程已关闭。"