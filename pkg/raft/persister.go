package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"kv/log"
	"os"
)

type PersistentState struct {
	CurrentTerm int
	VoteFor     int
	Logs        []LogEntry
}

func (rf *Raft) persist() {
	state := PersistentState{
		CurrentTerm: rf.currentTerm,
		VoteFor:     rf.voteFor,
		Logs:        rf.logs,
	}
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	err := encoder.Encode(state)
	if err != nil {
		panic(fmt.Sprintf("Failed to encode state with gob: %v", err))
	}
	rf.Save(buffer.Bytes(), nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist() {
	data, err := os.ReadFile(rf.statePath)
	if err != nil {
		fmt.Println("No previous state found")
		return
	}
	if len(data) == 0 {
		fmt.Println("Empty log file, no state to restore")
		return
	}
	buffer := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buffer)

	var state PersistentState
	err = decoder.Decode(&state)
	if err != nil {
		fmt.Println("Failed to decode persisted state:", err)
		return
	}
	rf.currentTerm = state.CurrentTerm
	rf.voteFor = state.VoteFor
	rf.logs = state.Logs
	rf.lastApplied, rf.commitIndex = rf.getFirstLog().Index, rf.getFirstLog().Index
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	snapshotIndex := rf.getFirstLog().Index
	if index <= snapshotIndex || index > rf.getLastLog().Index {
		return
	}

	// 保留 index 之后的日志
	newLogs := make([]LogEntry, len(rf.logs[index-snapshotIndex:]))
	copy(newLogs, rf.logs[index-snapshotIndex:])
	rf.logs = newLogs
	rf.logs[0].Command = nil // dummy 入口

	// 编码 raft 状态
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	state := PersistentState{
		CurrentTerm: rf.currentTerm,
		VoteFor:     rf.voteFor,
		Logs:        rf.logs,
	}
	err := encoder.Encode(state)
	if err != nil {
		log.DPrintf("Snapshot(): gob encode failed: %v", err)
	}
	raftstate := buffer.Bytes()

	rf.Save(raftstate, snapshot)

	log.DPrintf("Server %d generated snapshot at index %d", rf.me, index)
}

func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 1. 过期快照（比 commitIndex 还旧）
	if lastIncludedIndex <= rf.commitIndex {
		return false
	}

	// 2. 截断日志（全部被快照覆盖）
	if lastIncludedIndex > rf.getLastLog().Index {
		rf.logs = make([]LogEntry, 1)
	} else {
		rf.logs = rf.logs[lastIncludedIndex-rf.getFirstLog().Index:]
		rf.logs[0].Command = nil
	}
	rf.logs[0].Term = lastIncludedTerm
	rf.logs[0].Index = lastIncludedIndex

	// 3. 更新 commitIndex、lastApplied
	rf.commitIndex = lastIncludedIndex
	rf.lastApplied = lastIncludedIndex

	// 4. 序列化 raft 状态（term, vote, logs）
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	state := PersistentState{
		CurrentTerm: rf.currentTerm,
		VoteFor:     rf.voteFor,
		Logs:        rf.logs,
	}
	err := encoder.Encode(state)
	if err != nil {
		log.DPrintf("CondInstallSnapshot(): gob encode failed: %v", err)
	}
	raftstate := buffer.Bytes()

	rf.Save(raftstate, snapshot)

	log.DPrintf("Server %d: installed snapshot at index %d", rf.me, lastIncludedIndex)
	return true
}

func (rf *Raft) ReadSnapshot() []byte {
	data, err := os.ReadFile(rf.snapshotPath)
	if os.IsNotExist(err) {
		log.DPrintf("Raft %d: no snapshot found", rf.me)
		return nil
	} else if err != nil {
		log.DPrintf("Raft %d: ReadSnapshot() failed: %v", rf.me, err)
		return nil
	}
	return data
}

func (rf *Raft) Save(raftstate []byte, snapshot []byte) {
	err := os.WriteFile(rf.statePath, raftstate, 0644)
	if err != nil {
		log.DPrintf("Failed to write raft state: %v", err)
	}

	if snapshot != nil {
		err = os.WriteFile(rf.snapshotPath, snapshot, 0644)
		if err != nil {
			log.DPrintf("Failed to write snapshot: %v", err)
		}
	}
}
