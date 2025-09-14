package raft

import (
	"kv/log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"kv/pkg/proto/kvpb"
	"kv/pkg/proto/raftpb"
	"kv/pkg/wal"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type LogEntry struct {
	Command []byte
	Term    int
	Index   int
}

type ApplyMsg struct {
	CommandValid bool
	Command      []byte
	CommandIndex int
	CommandTerm  int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu                                    sync.RWMutex
	peers                                 map[int]raftpb.RaftServiceClient
	peerAddrs                             map[int]string
	statePath                             string
	snapshotPath                          string
	me                                    int
	dead                                  int32
	currentTerm                           int
	voteFor                               int
	logs                                  []LogEntry
	commitIndex                           int
	lastApplied                           int
	state                                 string
	nextIndex                             []int
	matchIndex                            []int
	electionTimer                         *time.Timer
	heartbeatTimer                        *time.Timer
	applyCh                               chan ApplyMsg
	applyCond                             *sync.Cond
	replicatorCond                        []*sync.Cond
	walManager                            *wal.WALManager // 新增：WAL管理器
	raftpb.UnimplementedRaftServiceServer                 // gRPC服务端接口嵌入
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()

	return rf.currentTerm, rf.state == "Leader"
}

func (rf *Raft) Start(command []byte) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := rf.getLastLog().Index + 1
	isLeader := rf.state == "Leader"
	log.DPrintf("server %d, Term %d, Starting command: %v", rf.me, rf.currentTerm, command)

	if isLeader {
		logEntry := LogEntry{
			Command: command,
			Term:    rf.currentTerm,
			Index:   index,
		}
		walEntry := &kvpb.WALEntry{
			Term:  uint64(rf.currentTerm),
			Index: uint64(index),
			Type:  kvpb.EntryType_ENTRY_NORMAL,
			Data:  command,
		}
		if rf.walManager != nil {
			if err := rf.walManager.WriteEntry(walEntry); err != nil {
				log.DPrintf("WAL 写入失败，拒绝追加日志: %v", err)
				return -1, rf.currentTerm, false
			}
		}
		rf.logs = append(rf.logs, logEntry)
		rf.persist()
		rf.nextIndex[rf.me] = index + 1
		rf.matchIndex[rf.me] = index
		log.DPrintf("Leader %d, Term %d, Appended log: Index=%d, Command=%v", rf.me, rf.currentTerm, index, command)

		go rf.BroadcastHeartbeat(false)
	} else {
		log.DPrintf("Not a leader, returning")
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
			go rf.broadcastAppendEntries(peer)
		} else {
			// just need to signal replicator to send log entries to peer
			rf.replicatorCond[peer].Signal()
		}
	}
}

func (rf *Raft) needReplicating(peer int) bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
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
		rf.broadcastAppendEntries(peer)
		// time.Sleep(time.Duration(100) * time.Millisecond) //大量输出日志所以需要sleep
	}
}

func (rf *Raft) broadcastAppendEntries(i int) {
	log.DPrintf("Leader %d broadcasting AppendEntries, Term %d", rf.me, rf.currentTerm)
	term := rf.currentTerm

	rf.mu.RLock()
	if rf.state != "Leader" || rf.currentTerm != term {
		rf.mu.RUnlock()
		return
	}

	prevLogIndex := rf.nextIndex[i] - 1

	if prevLogIndex < rf.getFirstLog().Index {
		firstLog := rf.getFirstLog()
		args := &raftpb.InstallSnapshotArgs{
			Term:              int32(rf.currentTerm),
			LeaderId:          int32(rf.me),
			LastIncludedIndex: int32(firstLog.Index),
			LastIncludedTerm:  int32(firstLog.Term),
			Data:              rf.ReadSnapshot(),
		}
		rf.mu.RUnlock()

		reply := new(raftpb.InstallSnapshotReply)

		if rf.sendInstallSnapshot(i, args, reply) {
			rf.mu.Lock()
			if rf.state == "Leader" && rf.currentTerm == int(args.Term) {
				if int(reply.Term) > rf.currentTerm {
					rf.currentTerm = int(reply.Term)
					rf.state = "Follower"
					rf.voteFor = -1
					rf.persist()
					resetTimer(rf.electionTimer, time.Duration(randomInRange(1000, 2000))*time.Millisecond)
					rf.heartbeatTimer.Stop()

				} else {
					rf.nextIndex[i] = int(args.LastIncludedIndex) + 1
					rf.matchIndex[i] = int(args.LastIncludedIndex)
					rf.replicatorCond[i].Signal()
				}
			}
			rf.mu.Unlock()
			log.DPrintf("{Node %v} sends InstallSnapshotArgs %v to {Node %v} and receives InstallSnapshotReply %v", rf.me, args, i, reply)
		}
	} else {
		prevLogTerm := rf.logs[prevLogIndex-rf.getFirstLog().Index].Term

		entries := make([]LogEntry, len(rf.logs[prevLogIndex-rf.getFirstLog().Index+1:]))
		copy(entries, rf.logs[prevLogIndex-rf.getFirstLog().Index+1:])

		args := raftpb.AppendEntriesArgs{
			Term:         int32(term),
			LeaderId:     int32(rf.me),
			PrevLogIndex: int32(prevLogIndex),
			PrevLogTerm:  int32(prevLogTerm),
			Entries:      make([]*raftpb.LogEntry, len(entries)),
			LeaderCommit: int32(rf.commitIndex),
		}
		for j := range entries {
			args.Entries[j] = &raftpb.LogEntry{
				Command: entries[j].Command,
				Term:    int32(entries[j].Term),
				Index:   int32(entries[j].Index),
			}
		}
		rf.mu.RUnlock()

		log.DPrintf("Leader %d sending AppendEntries to Follower %d: PrevLogIndex=%d, Entries=%v LeaderCommit=%d",
			rf.me, i, args.PrevLogIndex, args.Entries, args.LeaderCommit)

		reply := raftpb.AppendEntriesReply{}

		if rf.sendAppendEntries(i, &args, &reply) {
			rf.mu.Lock()
			if int(reply.Term) > rf.currentTerm {
				log.DPrintf("Leader %d sees higher term from Follower %d: Term %d", rf.me, i, reply.Term)

				// rf.logs = rf.logs[:len(rf.logs)-1]
				rf.currentTerm = int(reply.Term)
				rf.state = "Follower"
				rf.voteFor = -1
				rf.persist()
				resetTimer(rf.electionTimer, time.Duration(randomInRange(1000, 2000))*time.Millisecond)
				rf.heartbeatTimer.Stop()

				rf.mu.Unlock()
				return
			} else {
				if reply.Success {
					match := args.PrevLogIndex + int32(len(args.Entries))
					rf.matchIndex[i] = int(match)
					rf.nextIndex[i] = int(match) + 1
					log.DPrintf("Follower %d successfully replicated log, nextIndex=%d", i, rf.nextIndex[i])
					rf.updateCommitIndex()
					rf.mu.Unlock()
					return
				} else {
					// if rf.nextIndex[i] > 1 {
					// 	rf.nextIndex[i]--
					// }
					// log.DPrintf("Follower %d failed to replicate log, retrying with PrevLogIndex=%d", i, rf.nextIndex[i])
					if reply.XLen == 0 && reply.XTerm == 0 {
						rf.mu.Unlock()
						return
					}

					if reply.XTerm == -1 {
						// Follower 日志过短
						rf.nextIndex[i] = int(reply.XLen)
					} else {
						rf.nextIndex[i] = int(reply.XIndex)
					}

					rf.nextIndex[i] = Max(rf.nextIndex[i], 1)
				}
			}
			rf.mu.Unlock()
		}
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
				// log.DPrintf("ID=%d, matchIndex=%d", j, rf.matchIndex[j])
				count++
			}
		}
		if count > len(rf.peers)/2 {
			rf.commitIndex = i
			rf.applyCond.Signal()
			log.DPrintf("Leader %d successfully update commitIndex. New commitIndex=%d", rf.me, rf.commitIndex)
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

		log.DPrintf("server %d applier: Applying logs from index %d to %d", rf.me, lastApplied+1, commitIndex)

		for _, entry := range entries {
			log.DPrintf("server %d applier: Applying log: Index=%d, Command=%v", rf.me, entry.Index, entry.Command)
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
				CommandTerm:  entry.Term,
			}
		}
		rf.mu.Lock()
		rf.lastApplied = Max(rf.lastApplied, commitIndex)
		log.DPrintf("server %d applier: Finished applying logs up to commitIndex=%d", rf.me, rf.commitIndex)
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
				resetTimer(rf.electionTimer, time.Duration(randomInRange(1000, 2000))*time.Millisecond)
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
	log.DPrintf("Server %d start election", rf.me)

	rf.mu.Lock()
	rf.currentTerm++
	rf.state = "Candidate"
	rf.voteFor = rf.me
	rf.persist()

	voteCount := 1
	args := raftpb.RequestVoteArgs{
		Term:         int32(rf.currentTerm),
		CandidateId:  int32(rf.me),
		LastLogIndex: int32(rf.getLastLog().Index),
		LastLogTerm:  int32(rf.getLastLog().Term),
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

			reply := raftpb.RequestVoteReply{}
			if rf.sendRequestVote(i, &args, &reply) {
				rf.mu.Lock()

				if reply.VoteGranted {
					voteCount++
					if voteCount > len(rf.peers)/2 && rf.state == "Candidate" {
						rf.state = "Leader"
						log.DPrintf("server %d become Leader", rf.me)
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
					if int(reply.Term) > rf.currentTerm {
						rf.currentTerm = int(reply.Term)
						rf.state = "Follower"
						rf.voteFor = -1
						rf.persist()
						resetTimer(rf.electionTimer, time.Duration(randomInRange(1000, 2000))*time.Millisecond)
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

// gRPC 连接所有其他 Raft 节点
func (rf *Raft) connectToPeersGRPC() {
	rf.peers = make(map[int]raftpb.RaftServiceClient)
	for id, addr := range rf.peerAddrs {
		if id == rf.me {
			continue
		}
		go func(id int, addr string) {
			maxRetries := 10
			retryCount := 0
			for retryCount < maxRetries {
				// 使用新的 grpc.NewClient API
				client, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
				if err == nil {
					raftClient := raftpb.NewRaftServiceClient(client)
					rf.mu.Lock()
					rf.peers[id] = raftClient
					rf.mu.Unlock()
					log.Printf("gRPC连接%d号节点成功", id)
					return // gRPC 长连接，无需保活循环
				}
				retryCount++
				log.Printf("gRPC连接%d号节点失败，重试 %d/%d: %v", id, retryCount, maxRetries, err)
				time.Sleep(500 * time.Millisecond)
			}
			log.Printf("gRPC连接%d号节点最终失败", id)
		}(id, addr)
	}
}

func Make(me int, peerAddrs map[int]string, myAddr string, applyCh chan ApplyMsg, statePath string, snapshotPath string, walManager *wal.WALManager) *Raft {
	rf := &Raft{
		mu:             sync.RWMutex{},
		peerAddrs:      peerAddrs,
		statePath:      statePath,
		snapshotPath:   snapshotPath,
		me:             me,
		dead:           0,
		currentTerm:    0,
		voteFor:        -1,
		logs:           make([]LogEntry, 1), // dummy entry at index 0
		commitIndex:    0,
		lastApplied:    0,
		nextIndex:      make([]int, len(peerAddrs)),
		matchIndex:     make([]int, len(peerAddrs)),
		state:          "Follower",
		electionTimer:  time.NewTimer(time.Duration(randomInRange(1000, 2000)) * time.Millisecond),
		heartbeatTimer: time.NewTimer(time.Duration(125) * time.Millisecond),
		applyCh:        applyCh,
		replicatorCond: make([]*sync.Cond, len(peerAddrs)),
		walManager:     walManager,
	}

	// 恢复持久化状态
	rf.readPersist()
	rf.applyCond = sync.NewCond(&rf.mu)

	// 连接所有其他节点
	rf.connectToPeersGRPC()

	// 启动各种协程
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

// WaitForIndex 等待特定索引被应用（用于同步操作）
func (rf *Raft) WaitForIndex(index int, timeout time.Duration) bool {
	startTime := time.Now()
	for time.Since(startTime) < timeout {
		rf.mu.RLock()
		if rf.lastApplied >= index {
			rf.mu.RUnlock()
			return true
		}
		rf.mu.RUnlock()
		time.Sleep(1 * time.Millisecond)
	}
	return false
}

// ServeGRPC 启动 gRPC 服务端
func (rf *Raft) ServeGRPC(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	s := grpc.NewServer()
	raftpb.RegisterRaftServiceServer(s, rf)
	return s.Serve(lis)
}
