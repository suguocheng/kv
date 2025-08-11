package raft

import (
	"context"
	"kv/log"
	"kv/pkg/proto/kvpb"
	"kv/pkg/proto/raftpb"
	"time"
)

// 实现 gRPC 服务端接口
func (rf *Raft) RequestVote(ctx context.Context, req *raftpb.RequestVoteArgs) (*raftpb.RequestVoteReply, error) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	resp := &raftpb.RequestVoteReply{}

	if req.Term < int32(rf.currentTerm) {
		resp.Term = int32(rf.currentTerm)
		resp.VoteGranted = false
		return resp, nil
	}

	if req.Term > int32(rf.currentTerm) {
		log.DPrintf("Follower %d sees higher term from Candidate %d: Term %d", rf.me, req.CandidateId, req.Term)
		rf.currentTerm = int(req.Term)
		rf.voteFor = -1
		rf.state = "Follower"
		rf.persist()
		resetTimer(rf.electionTimer, time.Duration(randomInRange(1000, 2000))*time.Millisecond)
		rf.heartbeatTimer.Stop()
	}

	resp.Term = int32(rf.currentTerm)

	if (rf.voteFor == -1 || rf.voteFor == int(req.CandidateId)) && rf.isLogUpToDate(int(req.LastLogIndex), int(req.LastLogTerm)) {
		rf.voteFor = int(req.CandidateId)
		resp.VoteGranted = true
		rf.persist()
		resetTimer(rf.electionTimer, time.Duration(randomInRange(1000, 2000))*time.Millisecond)
		log.DPrintf("Follower %d vote for Candidate %d", rf.me, req.CandidateId)
	} else {
		resp.VoteGranted = false
		log.DPrintf("Follower %d don't vote for Candidate %d", rf.me, req.CandidateId)
	}

	return resp, nil
}

func (rf *Raft) sendRequestVote(server int, args *raftpb.RequestVoteArgs, reply *raftpb.RequestVoteReply) bool {
	rf.mu.RLock()
	client, ok := rf.peers[server]
	rf.mu.RUnlock()
	if !ok {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := client.RequestVote(ctx, args)
	if err != nil {
		log.Printf("gRPC RequestVote to %d failed: %v", server, err)
		return false
	}
	reply.Term = resp.Term
	reply.VoteGranted = resp.VoteGranted
	return true
}

func (rf *Raft) AppendEntries(ctx context.Context, req *raftpb.AppendEntriesArgs) (*raftpb.AppendEntriesReply, error) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	resp := &raftpb.AppendEntriesReply{}

	log.DPrintf("Follower %d received Leader %d AppendEntries: PrevLogIndex=%d, PrevLogTerm=%d, Entries=%v, commitIndex=%d",
		rf.me, req.LeaderId, req.PrevLogIndex, req.PrevLogTerm, req.Entries, req.LeaderCommit)

	if req.Term >= int32(rf.currentTerm) {
		log.DPrintf("Follower %d term %d is outdated, switching to follower", rf.me, req.Term)
		rf.currentTerm = int(req.Term)
		rf.voteFor = -1
		rf.state = "Follower"
		rf.persist()
		resetTimer(rf.electionTimer, time.Duration(randomInRange(1000, 2000))*time.Millisecond)
		rf.heartbeatTimer.Stop()

		firstIndex := rf.getFirstLog().Index
		lastIndex := rf.getLastLog().Index

		if int(req.PrevLogIndex) < firstIndex {
			resp.XTerm = -1
			resp.XIndex = int32(firstIndex)
			resp.Success = false
			log.DPrintf("Follower %d: PrevLogIndex %d < firstIndex %d",
				rf.me, req.PrevLogIndex, firstIndex)
			return resp, nil
		}

		if int(req.PrevLogIndex) > lastIndex {
			resp.XTerm = -1
			resp.XLen = int32(lastIndex + 1)
			resp.Success = false
			log.DPrintf("Follower %d: PrevLogIndex %d > lastIndex %d",
				rf.me, req.PrevLogIndex, lastIndex)
			return resp, nil
		}

		if rf.logs[int(req.PrevLogIndex)-firstIndex].Term != int(req.PrevLogTerm) {
			resp.XTerm = int32(rf.logs[int(req.PrevLogIndex)-firstIndex].Term)
			resp.XIndex = req.PrevLogIndex
			for i := int(req.PrevLogIndex) - 1; i >= firstIndex; i-- {
				if rf.logs[i-firstIndex].Term != int(resp.XTerm) {
					break
				}
				resp.XIndex = int32(i)
			}
			resp.Success = false
			return resp, nil
		}

		newEntriesIndex := int(req.PrevLogIndex) + len(req.Entries)
		lastLogIndex := rf.getLastLog().Index

		if newEntriesIndex < lastLogIndex {
			isDuplicate := true
			for i, entry := range req.Entries {
				logIndex := int(req.PrevLogIndex) + 1 + i
				if rf.logs[logIndex-firstIndex].Term != int(entry.Term) {
					isDuplicate = false
					break
				}
			}
			if isDuplicate {
				resp.Term = int32(rf.currentTerm)
				resp.Success = false
				resp.XIndex = int32(len(rf.logs))
				log.DPrintf("follower %d received duplicate logs, rejecting", rf.me)
				return resp, nil
			}
		}

		// 复制日志（修正：只对新增的 entry 写 WAL 和追加到内存日志）
		appendStart := 0
		for i, entry := range req.Entries {
			logIndex := int(entry.Index)
			if logIndex <= lastIndex && rf.logs[logIndex-firstIndex].Term == int(entry.Term) {
				// 已存在且 term 一致，跳过
				continue
			} else {
				appendStart = i
				break
			}
		}
		if appendStart < len(req.Entries) {
			// 先截断日志
			rf.logs = rf.logs[:int(req.PrevLogIndex)-firstIndex+1+appendStart]
			// 只写新增部分 WAL
			walEntries := make([]*kvpb.WALEntry, 0, len(req.Entries)-appendStart)
			for _, entry := range req.Entries[appendStart:] {
				walEntry := &kvpb.WALEntry{
					Term:  uint64(entry.Term),
					Index: uint64(entry.Index),
					Type:  kvpb.EntryType_ENTRY_NORMAL,
					Data:  entry.Command,
				}
				walEntries = append(walEntries, walEntry)
			}
			if rf.walManager != nil && len(walEntries) > 0 {
				for _, walEntry := range walEntries {
					if err := rf.walManager.WriteEntry(walEntry); err != nil {
						log.DPrintf("Follower %d WAL 写入失败，拒绝追加日志: %v", rf.me, err)
						resp.Term = int32(rf.currentTerm)
						resp.Success = false
						return resp, nil
					}
				}
			}
			// 追加到内存日志
			for _, entry := range req.Entries[appendStart:] {
				logEntry := LogEntry{
					Index:   int(entry.Index),
					Term:    int(entry.Term),
					Command: entry.Command,
				}
				rf.logs = append(rf.logs, logEntry)
			}
		}
		rf.persist()

		newCommitIndex := Min(int(req.LeaderCommit), rf.getLastLog().Index)
		if newCommitIndex > rf.commitIndex {
			rf.commitIndex = newCommitIndex
			rf.applyCond.Signal()
			log.DPrintf("Follower %d successfully update commitIndex. New commitIndex=%d", rf.me, rf.commitIndex)
		}

		resp.Term = int32(rf.currentTerm)
		resp.Success = true
	} else {
		resp.Term = int32(rf.currentTerm)
		resp.Success = false
	}
	return resp, nil
}

func (rf *Raft) sendAppendEntries(server int, args *raftpb.AppendEntriesArgs, reply *raftpb.AppendEntriesReply) bool {
	rf.mu.RLock()
	client, ok := rf.peers[server]
	rf.mu.RUnlock()
	if !ok {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := client.AppendEntries(ctx, args)
	if err != nil {
		log.Printf("gRPC AppendEntries to %d failed: %v", server, err)
		return false
	}
	reply.Term = resp.Term
	reply.Success = resp.Success
	reply.XTerm = resp.XTerm
	reply.XIndex = resp.XIndex
	reply.XLen = resp.XLen
	return true
}

func (rf *Raft) InstallSnapshot(ctx context.Context, req *raftpb.InstallSnapshotArgs) (*raftpb.InstallSnapshotReply, error) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	resp := &raftpb.InstallSnapshotReply{}
	resp.Term = int32(rf.currentTerm)

	if req.Term >= int32(rf.currentTerm) {
		rf.currentTerm = int(req.Term)
		rf.voteFor = -1
		rf.state = "Follower"
		rf.persist()
		resetTimer(rf.electionTimer, time.Duration(randomInRange(1000, 2000))*time.Millisecond)
		rf.heartbeatTimer.Stop()

		if int(req.LastIncludedIndex) <= rf.commitIndex {
			return resp, nil
		}

		go func() {
			rf.applyCh <- ApplyMsg{
				SnapshotValid: true,
				Snapshot:      req.Data,
				SnapshotTerm:  int(req.LastIncludedTerm),
				SnapshotIndex: int(req.LastIncludedIndex),
			}
		}()
		rf.applyCond.Signal()
	}
	return resp, nil
}

func (rf *Raft) sendInstallSnapshot(server int, args *raftpb.InstallSnapshotArgs, reply *raftpb.InstallSnapshotReply) bool {
	rf.mu.RLock()
	client, ok := rf.peers[server]
	rf.mu.RUnlock()
	if !ok {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := client.InstallSnapshot(ctx, args)
	if err != nil {
		log.Printf("gRPC InstallSnapshot to %d failed: %v", server, err)
		return false
	}
	reply.Term = resp.Term
	return true
}
