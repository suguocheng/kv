#!/bin/zsh

for i in {0..2}; do
  echo "Starting server $i..."
  go run cmd/server/main.go $i > log/test${i}.log 2>&1 &
done

echo "All servers started. Logs are in log/test0.log, log/test1.log, log/test2.log"