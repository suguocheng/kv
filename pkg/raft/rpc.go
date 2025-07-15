package raft

import (
	"kv/log"
	"kv/pkg/kvstore"
	"time"
)

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	XTerm   int
	XIndex  int
	XLen    int
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return nil
	}

	if args.Term > rf.currentTerm {
		log.DPrintf("Follower %d sees higher term from Candidate %d: Term %d", rf.me, args.CandidateId, args.Term)

		rf.currentTerm = args.Term
		rf.voteFor = -1
		rf.state = "Follower"
		rf.persist()
		resetTimer(rf.electionTimer, time.Duration(randomInRange(1000, 2000))*time.Millisecond)
		rf.heartbeatTimer.Stop()
	}

	reply.Term = rf.currentTerm

	if (rf.voteFor == -1 || rf.voteFor == args.CandidateId) && rf.isLogUpToDate(args.LastLogIndex, args.LastLogTerm) {
		rf.voteFor = args.CandidateId
		reply.VoteGranted = true
		rf.persist()
		resetTimer(rf.electionTimer, time.Duration(randomInRange(1000, 2000))*time.Millisecond)

		log.DPrintf("Follower %d vote for Candidate %d", rf.me, args.CandidateId)
	} else {
		reply.VoteGranted = false
		log.DPrintf("Follower %d don't vote for Candidate %d", rf.me, args.CandidateId)
	}

	return nil
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	client := rf.peers[server]
	err := client.Call("Raft.RequestVote", args, reply)
	return err == nil
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	log.DPrintf("Follower %d received Leader %d AppendEntries: PrevLogIndex=%d, PrevLogTerm=%d, Entries=%v, commitIndex=%d",
		rf.me, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.Entries, args.LeaderCommit)

	if args.Term >= rf.currentTerm {
		log.DPrintf("Follower %d term %d is outdated, switching to follower", rf.me, args.Term)

		rf.currentTerm = args.Term
		rf.voteFor = -1
		rf.state = "Follower"
		rf.persist()
		resetTimer(rf.electionTimer, time.Duration(randomInRange(1000, 2000))*time.Millisecond)
		rf.heartbeatTimer.Stop()

		// 比较日志
		// if args.PrevLogIndex >= len(rf.logs) || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		// 	log.DPrintf("Follower %d log mismatch: PrevLogIndex=%d, PrevLogTerm=%d", rf.me, args.PrevLogIndex, args.PrevLogTerm)
		// 	reply.Term = rf.currentTerm
		// 	reply.Success = false
		// 	return
		// }

		firstIndex := rf.getFirstLog().Index
		lastIndex := rf.getLastLog().Index

		if args.PrevLogIndex < firstIndex {
			// Leader的PrevLogIndex比我们的快照还旧
			reply.XTerm = -1
			reply.XIndex = firstIndex
			reply.Success = false
			log.DPrintf("Follower %d: PrevLogIndex %d < firstIndex %d",
				rf.me, args.PrevLogIndex, firstIndex)
			return nil
		}

		if args.PrevLogIndex > lastIndex {
			// Follower的日志不够长
			reply.XTerm = -1
			reply.XLen = lastIndex + 1 // 下一个期望的索引
			reply.Success = false
			log.DPrintf("Follower %d: PrevLogIndex %d > lastIndex %d",
				rf.me, args.PrevLogIndex, lastIndex)
			return nil
		}

		if rf.logs[args.PrevLogIndex-rf.getFirstLog().Index].Term != args.PrevLogTerm {
			log.DPrintf("Follower %d log mismatch: PrevLogIndex=%d, PrevLogTerm=%d", rf.me, args.PrevLogIndex, args.PrevLogTerm)
			reply.XTerm = rf.logs[args.PrevLogIndex-rf.getFirstLog().Index].Term
			reply.XIndex = args.PrevLogIndex
			// 回溯找到该 Term 的第一个索引
			for i := args.PrevLogIndex - 1; i >= rf.getFirstLog().Index; i-- {
				if rf.logs[i-rf.getFirstLog().Index].Term != reply.XTerm {
					break
				}
				reply.XIndex = i
			}
			reply.Success = false
			return nil
		}

		newEntriesIndex := args.PrevLogIndex + len(args.Entries)
		lastLogIndex := rf.getLastLog().Index

		if newEntriesIndex < lastLogIndex {
			// 检查新日志是否和 follower 当前日志冲突
			isDuplicate := true
			for i, entry := range args.Entries {
				logIndex := args.PrevLogIndex + 1 + i
				if rf.logs[logIndex-rf.getFirstLog().Index].Term != entry.Term {
					// 日志 term 不匹配，说明 Leader 需要覆盖 follower 的日志
					isDuplicate = false
					break
				}
			}

			// 如果没有 break，说明日志匹配，无需覆盖，拒绝重复 RPC
			if isDuplicate {
				reply.Term = rf.currentTerm
				reply.Success = false
				reply.XIndex = len(rf.logs)

				log.DPrintf("follower %d received duplicate logs, rejecting", rf.me)
				return nil
			}
		}

		// 复制日志
		// if args.Entries != nil {
		rf.logs = rf.logs[:args.PrevLogIndex-rf.getFirstLog().Index+1]
		rf.logs = append(rf.logs, args.Entries...)
		// 新增：写入WAL
		if rf.walManager != nil {
			for _, entry := range args.Entries {
				walEntry := &kvstore.WALEntry{
					Term:  uint64(entry.Term),
					Index: uint64(entry.Index),
					Type:  kvstore.EntryNormal,
					Data:  entry.Command,
				}
				rf.walManager.WriteEntry(walEntry)
			}
		}
		rf.persist()

		// for index, entry := range args.Entries {
		// 	// find the junction of the existing log and the appended log.
		// 	if entry.Index >= len(rf.logs) || rf.logs[entry.Index].Term != entry.Term {
		// 		rf.logs = append(rf.logs[:entry.Index], args.Entries[index:]...)
		// 		rf.persist()
		// 		break
		// 	}
		// }

		log.DPrintf("Follower %d copy successed: Entries=%v", rf.me, rf.logs)
		// }

		// 更新commitIndex
		newCommitIndex := Min(args.LeaderCommit, rf.getLastLog().Index)
		if newCommitIndex > rf.commitIndex {
			rf.commitIndex = Min(args.LeaderCommit, rf.getLastLog().Index)
			rf.applyCond.Signal()
			log.DPrintf("Follower %d successfully update commitIndex. New commitIndex=%d", rf.me, rf.commitIndex)
		}

		reply.Term = rf.currentTerm
		reply.Success = true
	} else {
		reply.Term = rf.currentTerm
		reply.Success = false
	}
	return nil
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	rf.mu.RLock()
	client := rf.peers[server]
	rf.mu.RUnlock()
	if client == nil {
		return false
	}
	err := client.Call("Raft.AppendEntries", args, reply)
	if err != nil {
		log.Printf("AppendEntries to %d failed: %v, 清理连接等待重连", server, err)
		rf.mu.Lock()
		delete(rf.peers, server)
		rf.mu.Unlock()
		return false
	}
	return true
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer log.DPrintf("{Node %v}'s state is {state %v, term %v}} after processing InstallSnapshot,  InstallSnapshotArgs %v and InstallSnapshotReply %v ", rf.me, rf.state, rf.currentTerm, args, reply)

	reply.Term = rf.currentTerm

	if args.Term >= rf.currentTerm {
		rf.currentTerm = args.Term
		rf.voteFor = -1
		rf.state = "Follower"
		rf.persist()
		resetTimer(rf.electionTimer, time.Duration(randomInRange(1000, 2000))*time.Millisecond)
		rf.heartbeatTimer.Stop()

		// check the snapshot is more up-to-date than the current log
		if args.LastIncludedIndex <= rf.commitIndex {
			return nil
		}

		go func() {
			rf.applyCh <- ApplyMsg{
				SnapshotValid: true,
				Snapshot:      args.Data,
				SnapshotTerm:  args.LastIncludedTerm,
				SnapshotIndex: args.LastIncludedIndex,
			}
		}()

		rf.applyCond.Signal()

	}

	return nil
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	client := rf.peers[server]
	err := client.Call("Raft.InstallSnapshot", args, reply)
	return err == nil
}
