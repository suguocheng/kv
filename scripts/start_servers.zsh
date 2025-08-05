#!/bin/zsh

# 检查配置文件是否存在
if [ ! -f "../config.env" ]; then
    echo "Config file not found. Creating from example..."
    cp ../config.example.env ../config.env
    echo "Please edit config.env if needed, then run this script again."
    exit 1
fi

echo "Starting servers with config from config.env..."

for i in {0..2}; do
  echo "Starting server $i..."
  go run ../cmd/server/main.go $i > ../log/test${i}.log 2>&1 &
done

echo "All servers started. Logs are in log/test0.log, log/test1.log, log/test2.log"
echo "Use 'tail -f log/test*.log' to monitor logs"