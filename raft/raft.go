package raft

import (
	"encoding/gob"
	"encoding/json"
	"fmt"
	"kv/kvstore"
	"log"
	"net"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

func init() {
	// 假设你传的是 kvstore.Op 类型
	gob.Register(kvstore.Op{})
}

type LogEntry struct {
	Command interface{}
	Term    int
	Index   int
}

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int

	// For 3D:
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

// A Go object implementing a single Raft peer.
type Raft struct {
	mu             sync.Mutex
	peers          map[int]*rpc.Client
	peerAddrs      map[int]string
	logPath        string
	me             int
	dead           int32
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
	replicatorCond []*sync.Cond
}

// return currentTerm and whether this server
// believes it is the leader.
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

// restore previously persisted state.
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

		go rf.BroadcastHeartbeat(false)
	} else {
		DPrintf("Not a leader, returning")
	}

	return index, rf.currentTerm, isLeader
}

func (rf *Raft) BroadcastHeartbeat(isHeartbeat bool) {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		if isHeartbeat {
			// should send heartbeat to all peers immediately
			go rf.broadcastAppendEntries(true, peer)
		} else {
			// just need to signal replicator to send log entries to peer
			rf.replicatorCond[peer].Signal()
		}
	}
}
func (rf *Raft) needReplicating(peer int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// check the logs of peer is behind the leader
	return rf.state == "Leader" && rf.matchIndex[peer] < rf.getLastLog().Index
}

func (rf *Raft) replicator(peer int) {
	rf.replicatorCond[peer].L.Lock()
	defer rf.replicatorCond[peer].L.Unlock()
	for !rf.killed() {
		for !rf.needReplicating(peer) {
			rf.replicatorCond[peer].Wait()
		}
		// send log entries to peer
		rf.broadcastAppendEntries(false, peer)
		time.Sleep(time.Duration(100) * time.Millisecond)
	}
}

func (rf *Raft) broadcastAppendEntries(isHeartbeat bool, i int) {
	DPrintf("Leader %d broadcasting AppendEntries, Term %d, isHeartbeat %v", rf.me, rf.currentTerm, isHeartbeat)
	term := rf.currentTerm

	rf.mu.Lock()
	if rf.state != "Leader" || rf.currentTerm != term {
		rf.mu.Unlock()
		return
	}

	prevLogIndex := rf.nextIndex[i] - 1
	prevLogTerm := rf.logs[prevLogIndex-rf.getFirstLog().Index].Term
	var entries []LogEntry
	if !isHeartbeat {
		entries = make([]LogEntry, len(rf.logs[prevLogIndex-rf.getFirstLog().Index+1:]))
		copy(entries, rf.logs[prevLogIndex-rf.getFirstLog().Index+1:])
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
				// if rf.nextIndex[i] > 1 {
				// 	rf.nextIndex[i]--
				// }
				// DPrintf("Follower %d failed to replicate log, retrying with PrevLogIndex=%d", i, rf.nextIndex[i])
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
	}
}

func (rf *Raft) updateCommitIndex() {
	for i := rf.getLastLog().Index; i > rf.commitIndex; i-- {
		if rf.logs[i-rf.getFirstLog().Index].Term != rf.currentTerm {
			continue // 提前跳过非当前 Term 的日志
		}
		count := 1 // 包括自己
		for j := range rf.peers {
			if j == rf.me {
				continue
			}
			if rf.matchIndex[j] >= i {
				// DPrintf("ID=%d, matchIndex=%d", j, rf.matchIndex[j])
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
		copy(entries, rf.logs[lastApplied-rf.getFirstLog().Index+1:commitIndex-rf.getFirstLog().Index+1])
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

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here (3A)
		// Check if a leader election should be started.
		select {
		case <-rf.electionTimer.C:
			rf.startElection()
			if rf.state != "Leader" {
				resetTimer(rf.electionTimer, time.Duration(randomInRange(500, 1000))*time.Millisecond)
			}
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.state == "Leader" {
				rf.BroadcastHeartbeat(true)
				resetTimer(rf.heartbeatTimer, time.Duration(125)*time.Millisecond)
			}
			rf.mu.Unlock()
		}
		// pause for a random amount of time between 50 and 350
		// milliseconds.
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
						rf.BroadcastHeartbeat(true)
						rf.electionTimer.Stop()
						resetTimer(rf.heartbeatTimer, time.Duration(125)*time.Millisecond)

						// 初始化nextIndex和matchIndex
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
	// if voteCount <= len(rf.peers)/2 {
	// 	rf.currentTerm--
	// 	rf.state = "Follower"
	// 	rf.voteFor = -1
	// }
}

func Make(me int, peerAddrs map[int]string, myAddr string, applyCh chan ApplyMsg, logFile string) *Raft {
	rf := &Raft{}
	rf.me = me
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.logs = make([]LogEntry, 1)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peerAddrs))
	rf.matchIndex = make([]int, len(peerAddrs))
	rf.peerAddrs = peerAddrs
	rf.state = "Follower"
	rf.logPath = logFile
	rf.mu = sync.Mutex{}
	rf.electionTimer = time.NewTimer(time.Duration(randomInRange(1000, 2000)) * time.Millisecond)
	rf.heartbeatTimer = time.NewTimer(time.Duration(125) * time.Millisecond)
	rf.heartbeatTimer.Stop()

	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.replicatorCond = make([]*sync.Cond, len(peerAddrs))

	// initialize from state persisted before a crash
	rf.readPersist()

	// 启动本地 RPC 监听
	go func() {
		rpc.Register(rf)
		raftAddr := peerAddrs[me] // 使用 Raft 通信端口（如8000）
		ln, err := net.Listen("tcp", raftAddr)
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
		go func(id int, addr string) {
			retries := 0
			maxRetries := 5
			for retries < maxRetries {
				client, err := rpc.Dial("tcp", addr)
				if err == nil {
					rf.mu.Lock()
					rf.peers[id] = client
					rf.mu.Unlock()
					return
				}
				time.Sleep(time.Duration(retries*100) * time.Millisecond)
				retries++
			}
			log.Printf("Raft %d failed to connect peer %d after retries", me, id)
		}(id, addr)
	}

	for id, _ := range peerAddrs {
		rf.matchIndex[id], rf.nextIndex[id] = 0, rf.getLastLog().Index+1
		if id != rf.me {
			rf.replicatorCond[id] = sync.NewCond(&sync.Mutex{})
			// start replicator goroutine to send log entries to peer
			go rf.replicator(id)
		}
	}

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()

	return rf
}
