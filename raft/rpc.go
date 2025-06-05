package raft

import (
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

	if args.Term > rf.currentTerm {
		DPrintf("Follower %d sees higher term from Candidate %d: Term %d", rf.me, args.CandidateId, args.Term)

		rf.currentTerm = args.Term
		rf.voteFor = -1
		rf.state = "Follower"
		rf.persist()
		resetTimer(rf.electionTimer, time.Duration(randomInRange(500, 1000))*time.Millisecond)
	}

	if args.Term == rf.currentTerm {
		if (rf.voteFor == -1 || rf.voteFor == args.CandidateId) && rf.isLogUpToDate(args.LastLogIndex, args.LastLogTerm) {
			rf.voteFor = args.CandidateId
			reply.VoteGranted = true
			rf.state = "Follower"
			rf.persist()
			resetTimer(rf.electionTimer, time.Duration(randomInRange(500, 1000))*time.Millisecond)

			DPrintf("Follower %d vote for Candidate %d", rf.me, args.CandidateId)
		} else {
			reply.VoteGranted = false
			DPrintf("Follower %d don't vote for Candidate %d", rf.me, args.CandidateId)
		}
	} else {
		reply.VoteGranted = false
		DPrintf("Follower %d don't vote for Candidate %d", rf.me, args.CandidateId)
	}

	reply.Term = rf.currentTerm
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

	DPrintf("Follower %d received Leader %d AppendEntries: PrevLogIndex=%d, PrevLogTerm=%d, Entries=%v, commitIndex=%d",
		rf.me, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.Entries, args.LeaderCommit)

	if args.Term >= rf.currentTerm {
		DPrintf("Follower %d term %d is outdated, switching to follower", rf.me, args.Term)

		rf.currentTerm = args.Term
		rf.voteFor = -1
		rf.state = "Follower"
		rf.persist()
		resetTimer(rf.electionTimer, time.Duration(randomInRange(500, 1000))*time.Millisecond)

		// 检查日志是否匹配
		if args.PrevLogIndex >= len(rf.logs) {
			DPrintf("Follower %d log mismatch: PrevLogIndex=%d, PrevLogTerm=%d", rf.me, args.PrevLogIndex, args.PrevLogTerm)
			reply.XLen = len(rf.logs)
			reply.XTerm = -1
			reply.Success = false
			return nil
		}

		if rf.logs[args.PrevLogIndex-rf.getFirstLog().Index].Term != args.PrevLogTerm {
			DPrintf("Follower %d log mismatch: PrevLogIndex=%d, PrevLogTerm=%d", rf.me, args.PrevLogIndex, args.PrevLogTerm)
			reply.XTerm = rf.logs[args.PrevLogIndex-rf.getFirstLog().Index].Term
			reply.XIndex = args.PrevLogIndex
			// 回溯找到该 Term 的第一个索引
			for i := args.PrevLogIndex - rf.getFirstLog().Index - 1; i >= rf.getFirstLog().Index; i-- {
				if rf.logs[i].Term != reply.XTerm {
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

				DPrintf("follower %d received duplicate logs, rejecting", rf.me)
				return nil
			}
		}

		// 复制日志
		rf.logs = rf.logs[:args.PrevLogIndex-rf.getFirstLog().Index+1]
		rf.logs = append(rf.logs, args.Entries...)
		rf.persist()

		DPrintf("Follower %d copy successed: Entries=%v", rf.me, rf.logs)

		// 更新commitIndex
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = Min(args.LeaderCommit, len(rf.logs)-1)
			rf.applyCond.Signal()
			DPrintf("Follower %d successfully update commitIndex. New commitIndex=%d", rf.me, rf.commitIndex)
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
	client := rf.peers[server]
	err := client.Call("Raft.AppendEntries", args, reply)
	if err != nil {
		DPrintf("[RPC] AppendEntries to %d failed: %v", server, err)
	}
	return err == nil
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer DPrintf("{Node %v}'s state is {state %v, term %v}} after processing InstallSnapshot,  InstallSnapshotArgs %v and InstallSnapshotReply %v ", rf.me, rf.state, rf.currentTerm, args, reply)

	if args.Term >= rf.currentTerm {
		rf.currentTerm = args.Term
		rf.voteFor = -1
		rf.state = "Follower"
		rf.persist()
		resetTimer(rf.electionTimer, time.Duration(randomInRange(500, 1000))*time.Millisecond)

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

	} else {
		reply.Term = rf.currentTerm
	}
	return nil
}
