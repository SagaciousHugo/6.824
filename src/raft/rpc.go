package raft

import (
	"context"
	"fmt"
)

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else {
		if rf.currentTerm < args.Term {
			rf.updateCurrentTerm(args.Term)
			rf.state = FOLLOWER
		}
		if rf.replyLogIsNewerOrEqual(args.LastLogTerm, args.LastLogIndex) {
			if ok := rf.vote(args.CandidateId); ok {
				reply.Term = rf.currentTerm
				reply.VoteGranted = true
			} else {
				reply.Term = rf.currentTerm
				reply.VoteGranted = false
			}
		} else {
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
		}
	}
	reply.LastLogIndex = rf.lastLogIndex
	reply.LastLogTerm = rf.getLogEntryTerm(rf.lastLogIndex)
	//DPrintf("server %d received from candidate %d args = %+v reply = %+v\n", rf.me, args.CandidateId, args, reply)
}

func (rf *Raft) sendRequestVote(ctx context.Context, server int, args *RequestVoteArgs, replyCh chan *RequestVoteReply) {
	var reply *RequestVoteReply
	res := make(chan bool, 1)
Loop:
	for {
		reply = &RequestVoteReply{}
		go func() {
			ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
			res <- ok
		}()
		select {
		case <-ctx.Done():
			return
		case ok := <-res:
			if ok {
				DPrintf("CANDIDATE %d send server %d requestVote args = %+v reply = %+v\n", rf.me, server, args, reply)
				replyCh <- reply
				break Loop
			}
		}
	}
}

func (rf *Raft) RequestAppendEntries(args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm > args.Term {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	} else {
		/*if rf.state == LEADER && rf.currentTerm == args.Term {
			panic(fmt.Errorf("there have two leaders %d %d in term %d", rf.me, args.LeaderId, args.Term))
		}*/
		rf.state = FOLLOWER
		rf.updateCurrentTerm(args.Term)
		rf.leaderId = args.LeaderId
		go func() {
			rf.heartBeat <- 1
		}()
		if args.PrevLogIndex > rf.lastLogIndex {
			reply.Success = false
			reply.Term = rf.currentTerm
			reply.ConflictIndex = Min(rf.committedIndex+1, rf.lastLogIndex)
			return
		} else if args.PrevLogIndex < rf.lastIncludedIndex {
			reply.Success = false
			reply.Term = rf.currentTerm
			reply.ConflictIndex = rf.lastIncludedIndex + 1
			DPrintf("server %d reject RequestAppendEntries args.PrevLogIndex < rf.lastIncludedIndex\n", rf.me)
			return
		} else if rf.getLogEntryTerm(args.PrevLogIndex) != args.PrevLogTerm {
			reply.Success = false
			reply.Term = rf.currentTerm
			reply.ConflictIndex = args.PrevLogIndex
			DPrintf("server %d reject RequestAppendEntries rf.getLogEntryTerm(args.PrevLogIndex) != args.PrevLogTerm\n", rf.me)
			return
		}
		// lastIncludedIndex <= lastApplied <= committedIndex <= lastLogIndex
		// prevLogIndex in range [lastIncludedIndex, lastLogIndex]
		i := 0
		conflictIndex := 0
		baseIndex := args.PrevLogIndex + 1
		logChanged := false
		for ; i < len(args.Entries); i++ {
			if rf.lastLogIndex < baseIndex+i {
				break
			}
			if rf.getLogEntryTerm(baseIndex+i) != args.Entries[i].Term {
				conflictIndex = args.PrevLogIndex + i + 1
				if conflictIndex <= rf.committedIndex {
					reply.Success = false
					reply.Term = rf.currentTerm
					reply.ConflictIndex = conflictIndex
					panic(fmt.Errorf("leader %d conflict %d with server %d committedIndex leader args %+v server rf log=%+v committedIndex= %d lastApplied=%d lastIncludeIndex=%d\n", args.LeaderId, rf.me, conflictIndex, args, rf.log[conflictIndex-1:], rf.committedIndex, rf.lastApplied, rf.lastIncludedIndex))
					return
				} else {
					rf.deleteLogEntries(conflictIndex - 1)
					logChanged = true
				}
				//DPrintf("server %d log conflictIndex = %d  with leader args = %+v server log = %v committed = %d\n", rf.me, conflictIndex, args, rf.log, rf.committedIndex)
				break
			}
		}
		for ; i < len(args.Entries); i++ {
			rf.log = append(rf.log, args.Entries[i])
			rf.lastLogIndex++
			logChanged = true
		}
		oldCommitted := rf.committedIndex
		if args.LeaderCommitted > rf.committedIndex {
			rf.committedIndex = Min(args.LeaderCommitted, rf.lastLogIndex)
		}
		if oldCommitted != rf.committedIndex {
			rf.persist()
			rf.notifyStateMachine(StateMachineNewCommitted)
		} else if logChanged {
			rf.persist()
		}
		reply.Success = true
		reply.Term = rf.currentTerm
		reply.ConflictIndex = 0
	}
}

func (rf *Raft) sendRequestAppendEntries(server int, args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.RequestAppendEntries", args, reply)
	/*if ok {
		DPrintf("LEADER %d send sever %d RequestAppendEntries ok = %v args = %+v reply = %+v\n", rf.me, server, ok, args, reply)
	}*/
	return ok
}

func (rf *Raft) RequestInstallSnapshot(args *RequestInstallSnapshotArgs, reply *RequestInstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		return
	}
	rf.updateCurrentTerm(args.Term)
	rf.state = FOLLOWER
	rf.leaderId = args.LeaderId
	reply.Term = rf.currentTerm
	go func() {
		rf.heartBeat <- 1
	}()
	if rf.lastIncludedIndex < args.LastIncludedIndex {
		if rf.lastLogIndex > args.LastIncludedIndex {
			rf.log = rf.log[args.LastIncludedIndex+1-rf.lastIncludedIndex-1:]
		} else {
			rf.log = nil
			rf.lastLogIndex = args.LastIncludedIndex
		}
		rf.lastIncludedIndex = args.LastIncludedIndex
		rf.lastIncludedTerm = args.LastIncludedTerm
		rf.lastApplied = args.LastIncludedIndex
		rf.persister.SaveStateAndSnapshot(rf.persister.ReadRaftState(), args.Data)
		rf.notifyStateMachine(StateMachineInstallSnapshotStart)
	}
}

func (rf *Raft) sendRequestInstallSnapshot(server int, args *RequestInstallSnapshotArgs, reply *RequestInstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.RequestInstallSnapshot", args, reply)
	/*if ok {
		args.Data = nil
		DPrintf("LEADER %d send sever %d Raft.RequestInstallSnapshot ok = %v args = %+v reply = %+v\n", rf.me, server, ok, args, reply)
	}*/
	return ok
}
