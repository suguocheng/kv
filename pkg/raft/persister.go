package raft

import (
	"fmt"
	"kv/log"
	"kv/pkg/raftpb"
	"os"

	"google.golang.org/protobuf/proto"
)

type PersistentState struct {
	CurrentTerm int
	VoteFor     int
	Logs        []LogEntry
}

func (rf *Raft) persist() {
	state := &raftpb.PersistentState{
		CurrentTerm: int32(rf.currentTerm),
		VoteFor:     int32(rf.voteFor),
	}

	for _, log := range rf.logs {
		state.Logs = append(state.Logs, &raftpb.LogEntry{
			Index:   int32(log.Index),
			Term:    int32(log.Term),
			Command: log.Command,
		})
	}

	data, err := proto.Marshal(state)
	if err != nil {
		panic(fmt.Sprintf("Failed to marshal PersistentState: %v", err))
	}

	rf.Save(data, nil)
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

	var state raftpb.PersistentState
	err = proto.Unmarshal(data, &state)
	if err != nil {
		fmt.Println("Failed to decode persisted state:", err)
		return
	}

	rf.currentTerm = int(state.CurrentTerm)
	rf.voteFor = int(state.VoteFor)

	rf.logs = make([]LogEntry, 0, len(state.Logs))
	for _, log := range state.Logs {
		rf.logs = append(rf.logs, LogEntry{
			Index:   int(log.Index),
			Term:    int(log.Term),
			Command: log.Command,
		})
	}

	rf.lastApplied, rf.commitIndex = rf.getFirstLog().Index, rf.getFirstLog().Index
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	snapshotIndex := rf.getFirstLog().Index
	if index <= snapshotIndex || index > rf.getLastLog().Index {
		return
	}

	rf.logs = append([]LogEntry(nil), rf.logs[index-snapshotIndex:]...)
	rf.logs[0].Command = nil // dummy

	state := &raftpb.PersistentState{
		CurrentTerm: int32(rf.currentTerm),
		VoteFor:     int32(rf.voteFor),
	}
	for _, log := range rf.logs {
		state.Logs = append(state.Logs, &raftpb.LogEntry{
			Index:   int32(log.Index),
			Term:    int32(log.Term),
			Command: log.Command,
		})
	}

	raftstate, err := proto.Marshal(state)
	if err != nil {
		log.DPrintf("Snapshot(): marshal error: %v", err)
	}

	rf.Save(raftstate, snapshot)
	log.DPrintf("Server %d generated snapshot at index %d", rf.me, index)
}

func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if lastIncludedIndex <= rf.commitIndex {
		return false
	}

	if lastIncludedIndex > rf.getLastLog().Index {
		rf.logs = make([]LogEntry, 1)
	} else {
		rf.logs = append([]LogEntry(nil), rf.logs[lastIncludedIndex-rf.getFirstLog().Index:]...)
		rf.logs[0].Command = nil
	}
	rf.logs[0].Term = lastIncludedTerm
	rf.logs[0].Index = lastIncludedIndex

	rf.commitIndex = lastIncludedIndex
	rf.lastApplied = lastIncludedIndex

	state := &raftpb.PersistentState{
		CurrentTerm: int32(rf.currentTerm),
		VoteFor:     int32(rf.voteFor),
	}
	for _, log := range rf.logs {
		state.Logs = append(state.Logs, &raftpb.LogEntry{
			Index:   int32(log.Index),
			Term:    int32(log.Term),
			Command: log.Command,
		})
	}

	raftstate, err := proto.Marshal(state)
	if err != nil {
		log.DPrintf("CondInstallSnapshot(): marshal error: %v", err)
	}
	rf.Save(raftstate, snapshot)

	log.DPrintf("Server %d installed snapshot at index %d", rf.me, lastIncludedIndex)
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
