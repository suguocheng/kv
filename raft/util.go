package raft

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"
)

var logger *log.Logger

func init() {
	// 创建带文件名和行号的 Logger
	logger = log.New(os.Stdout, "", log.LstdFlags|log.Lshortfile)
}

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) {
	if Debug {
		logger.Output(2, fmt.Sprintf(format, a...))
	}
}

func randomInRange(min, max int) int {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return r.Intn(max-min) + min
}

func resetTimer(t *time.Timer, d time.Duration) {
	if !t.Stop() {
		// 如果Stop返回false，计时器可能已经过期但没有被读，清理通道
		select {
		case <-t.C:
			// 消费过期的信号防止通道堵塞
		default:
			// 通道里没有信号，不做任何处理
		}
	}
	t.Reset(d)
}

func (rf *Raft) isLogUpToDate(candidateLastIndex int, candidateLastTerm int) bool {
	lastIndex := len(rf.logs) - 1    // 当前节点的最后一个日志索引
	lastTerm := rf.getLastLog().Term // 当前节点的最后一个日志任期

	// 比较日志条目任期
	if candidateLastTerm > lastTerm {
		return true
	}

	// 如果日志条目任期相同，则比较索引
	if candidateLastTerm == lastTerm && candidateLastIndex >= lastIndex {
		return true
	}

	return false
}

func Min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func Max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func (rf *Raft) getLastLog() LogEntry {
	return rf.logs[len(rf.logs)-1]
}

func (rf *Raft) getFirstLog() LogEntry {
	return rf.logs[0]
}
