package raft

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type LogEntry struct {
	Command interface{}
	Term    int
	Index   int
}

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandTerm  int
	CommandIndex int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type PersistentState struct {
	CurrentTerm int        `json:"currentTerm"`
	VoteFor     int        `json:"voteFor"`
	Logs        []LogEntry `json:"logs"`
}

type Raft struct {
	mu        sync.Mutex
	peers     map[int]*rpc.Client
	peerAddrs map[int]string
	logPath   string
	me        int
	dead      int32

	currentTerm    int
	voteFor        int
	logs           []LogEntry
	commitIndex    int
	lastApplied    int
	state          string
	nextIndex      []int
	matchIndex     []int
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
	applyCh        chan ApplyMsg
	applyCond      *sync.Cond
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.state == "Leader"
}

func (rf *Raft) persist() {
	state := PersistentState{
		CurrentTerm: rf.currentTerm,
		VoteFor:     rf.voteFor,
		Logs:        rf.logs,
	}
	data, err := json.Marshal(state)
	if err != nil {
		panic(err)
	}
	err = os.WriteFile(rf.logPath, data, 0644)
	if err != nil {
		fmt.Println("Failed to persist state:", err)
	}
}

func (rf *Raft) readPersist() {
	data, err := os.ReadFile(rf.logPath)
	if err != nil {
		fmt.Println("No previous state found")
		return
	}
	if len(data) == 0 {
		fmt.Println("Empty log file, no state to restore")
		return
	}
	var state PersistentState
	err = json.Unmarshal(data, &state)
	if err != nil {
		fmt.Println("Failed to unmarshal persisted state:", err)
		return
	}
	rf.currentTerm = state.CurrentTerm
	rf.voteFor = state.VoteFor
	rf.logs = state.Logs
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {

}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := rf.getLastLog().Index + 1
	isLeader := rf.state == "Leader"
	DPrintf("server %d, Term %d, Starting command: %v", rf.me, rf.currentTerm, command)

	if isLeader {
		rf.logs = append(rf.logs, LogEntry{
			Command: command,
			Term:    rf.currentTerm,
			Index:   index,
		})
		rf.persist()
		rf.nextIndex[rf.me] = index + 1
		rf.matchIndex[rf.me] = index
		DPrintf("Leader %d, Term %d, Appended log: Index=%d, Command=%v", rf.me, rf.currentTerm, index, command)

		go rf.broadcastAppendEntries(false)
	} else {
		DPrintf("Not a leader, returning")
	}

	return index, rf.currentTerm, isLeader
}

func (rf *Raft) broadcastAppendEntries(isHeartbeat bool) {
	DPrintf("Leader %d broadcasting AppendEntries, Term %d, isHeartbeat %v", rf.me, rf.currentTerm, isHeartbeat)
	term := rf.currentTerm

	for index := range rf.peers {
		if index == rf.me {
			continue
		}
		go func(i int) {
			retryInterval := 10 * time.Millisecond
			maxInterval := 1 * time.Second
			for {
				rf.mu.Lock()
				if rf.state != "Leader" || rf.currentTerm != term {
					rf.mu.Unlock()
					return
				}

				prevLogIndex := rf.nextIndex[i] - 1
				prevLogTerm := rf.logs[prevLogIndex].Term
				var entries []LogEntry
				if !isHeartbeat {
					entries = make([]LogEntry, len(rf.logs[prevLogIndex+1:]))
					copy(entries, rf.logs[prevLogIndex+1:])
				}

				args := AppendEntriesArgs{
					Term:         term,
					LeaderId:     rf.me,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  prevLogTerm,
					Entries:      entries,
					LeaderCommit: rf.commitIndex,
				}
				rf.mu.Unlock()

				DPrintf("Leader %d sending AppendEntries to Follower %d: PrevLogIndex=%d, Entries=%v LeaderCommit=%d",
					rf.me, i, args.PrevLogIndex, args.Entries, args.LeaderCommit)

				reply := AppendEntriesReply{}

				if rf.sendAppendEntries(i, &args, &reply) {
					rf.mu.Lock()
					if reply.Term > rf.currentTerm {
						DPrintf("Leader %d sees higher term from Follower %d: Term %d", rf.me, i, reply.Term)

						// rf.logs = rf.logs[:len(rf.logs)-1]
						rf.currentTerm = reply.Term
						rf.state = "Follower"
						rf.voteFor = -1
						rf.persist()
						resetTimer(rf.electionTimer, time.Duration(randomInRange(500, 1000))*time.Millisecond)
						rf.heartbeatTimer.Stop()

						rf.mu.Unlock()
						return
					} else {
						if reply.Success {
							match := args.PrevLogIndex + len(args.Entries)
							rf.matchIndex[i] = match
							rf.nextIndex[i] = match + 1
							DPrintf("Follower %d successfully replicated log, nextIndex=%d", i, rf.nextIndex[i])
							rf.updateCommitIndex()
							rf.mu.Unlock()
							return
						} else {
							if reply.XLen == 0 && reply.XTerm == 0 {
								rf.mu.Unlock()
								return
							}

							if reply.XTerm == -1 {
								// Follower 日志过短
								rf.nextIndex[i] = reply.XLen
							} else {
								rf.nextIndex[i] = reply.XIndex
							}

							rf.nextIndex[i] = Max(rf.nextIndex[i], 1)
						}
					}
					rf.mu.Unlock()
				} else {
					time.Sleep(retryInterval)
					if 2*retryInterval > maxInterval {
						retryInterval = maxInterval
					} else {
						retryInterval = 2 * retryInterval
					}
				}

				if isHeartbeat {
					return
				}
			}
		}(index)
	}
}

func (rf *Raft) updateCommitIndex() {
	for i := len(rf.logs) - 1; i > rf.commitIndex; i-- {
		if rf.logs[i].Term != rf.currentTerm {
			continue
		}
		count := 1 // 包括自己
		for j := range rf.peers {
			if j == rf.me {
				continue
			}
			if rf.matchIndex[j] >= i {
				count++
			}
		}
		if count > len(rf.peers)/2 {
			rf.commitIndex = i
			rf.applyCond.Signal()
			DPrintf("Leader %d successfully update commitIndex. New commitIndex=%d", rf.me, rf.commitIndex)
			break
		}
	}
}

func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}

		commitIndex, lastApplied := rf.commitIndex, rf.lastApplied
		entries := make([]LogEntry, commitIndex-lastApplied)
		copy(entries, rf.logs[lastApplied+1:commitIndex+1])
		rf.mu.Unlock()

		DPrintf("server %d applier: Applying logs from index %d to %d", rf.me, lastApplied+1, commitIndex)

		for _, entry := range entries {
			DPrintf("server %d applier: Applying log: Index=%d, Command=%v", rf.me, entry.Index, entry.Command)
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
				CommandTerm:  entry.Term,
			}
		}
		rf.mu.Lock()
		rf.lastApplied = Max(rf.lastApplied, commitIndex)
		DPrintf("server %d applier: Finished applying logs up to commitIndex=%d", rf.me, rf.commitIndex)
		rf.mu.Unlock()
	}
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		select {
		case <-rf.electionTimer.C:
			rf.startElection()
			if rf.state != "Leader" {
				resetTimer(rf.electionTimer, time.Duration(randomInRange(500, 1000))*time.Millisecond)
			}
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.state == "Leader" {
				rf.broadcastAppendEntries(true)
				resetTimer(rf.heartbeatTimer, time.Duration(100)*time.Millisecond)
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) startElection() {
	DPrintf("Server %d start election", rf.me)

	rf.mu.Lock()
	rf.currentTerm++
	rf.state = "Candidate"
	rf.voteFor = rf.me
	rf.persist()

	voteCount := 1
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLog().Index,
		LastLogTerm:  rf.getLastLog().Term,
	}
	rf.mu.Unlock()
	for index := range rf.peers {
		if index == rf.me {
			continue
		}
		go func(i int) {

			rf.mu.Lock()
			if rf.state != "Candidate" {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()

			reply := RequestVoteReply{}
			if rf.sendRequestVote(i, &args, &reply) {
				rf.mu.Lock()

				if reply.VoteGranted {
					voteCount++
					if voteCount > len(rf.peers)/2 && rf.state == "Candidate" {
						rf.state = "Leader"
						DPrintf("server %d become Leader", rf.me)
						rf.broadcastAppendEntries(true)
						rf.electionTimer.Stop()
						resetTimer(rf.heartbeatTimer, time.Duration(100)*time.Millisecond)

						for index := range rf.nextIndex {
							if index == rf.me {
								continue
							}
							rf.nextIndex[index] = len(rf.logs)
							rf.matchIndex[index] = 0
						}
					}
				} else {
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.state = "Follower"
						rf.voteFor = -1
						rf.persist()
						resetTimer(rf.electionTimer, time.Duration(randomInRange(500, 1000))*time.Millisecond)
					}
				}
				rf.mu.Unlock()
			}
		}(index)
	}
}

func Make(me int, peerAddrs map[int]string, myAddr string, applyCh chan ApplyMsg, logFile string) *Raft {
	rf := &Raft{}
	rf.me = me
	rf.applyCh = applyCh
	rf.state = "Follower"
	rf.logs = make([]LogEntry, 1)
	rf.voteFor = -1
	rf.currentTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peerAddrs))
	rf.matchIndex = make([]int, len(peerAddrs))
	rf.peerAddrs = peerAddrs
	rf.logPath = logFile
	rf.mu = sync.Mutex{}
	rf.applyCond = sync.NewCond(&rf.mu)

	// timers
	rf.electionTimer = time.NewTimer(time.Duration(randomInRange(500, 1000)) * time.Millisecond)
	rf.heartbeatTimer = time.NewTimer(time.Duration(100) * time.Millisecond)
	rf.heartbeatTimer.Stop()

	// 读取本地持久化数据（JSON）
	rf.readPersist()

	// 启动本地 RPC 监听
	go func() {
		rpc.Register(rf)
		ln, err := net.Listen("tcp", myAddr)
		if err != nil {
			log.Fatalf("Raft %d listen error: %v", me, err)
		}
		for {
			conn, err := ln.Accept()
			if err == nil {
				go rpc.ServeConn(conn)
			}
		}
	}()

	// 连接 peers
	rf.peers = make(map[int]*rpc.Client)
	for id, addr := range peerAddrs {
		if id == me {
			continue
		}
		client, err := rpc.Dial("tcp", addr)
		if err != nil {
			log.Printf("Raft %d dial peer %d failed: %v", me, id, err)
			continue
		}
		rf.peers[id] = client
	}

	// 启动主循环
	go rf.ticker()
	go rf.applier()

	return rf
}
