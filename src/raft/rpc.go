package raft

import (
	"context"
)

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else {
		rf.updateCurrentTerm(args.Term)
		if rf.replyLogIsNewerOrEqual(args.LastLogTerm, args.LastLogIndex) {
			if ok := rf.vote(args.CandidateId); ok {
				rf.state = FOLLOWER
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
	reply.LastLogIndex = len(rf.log)
	if reply.LastLogIndex > 0 {
		reply.LastLogTerm = rf.log[reply.LastLogIndex-1].Term
	}
	DPrintf("server %d received from candidate %d args = %v reply = %v\n", rf.me, args.CandidateId, args, reply)
}

func (rf *Raft) sendRequestVote(ctx context.Context, server int, args *RequestVoteArgs, replyCh chan *RequestVoteReply) {
	var reply *RequestVoteReply
	res := make(chan bool)
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
		go func() {
			rf.heartBeat <- 1
		}()
		if args.PrevLogIndex > len(rf.log) || args.PrevLogIndex > 0 && rf.log[args.PrevLogIndex-1].Term != args.PrevLogTerm {
			reply.Success = false
			reply.Term = rf.currentTerm
			reply.ConflictIndex = Min(rf.committedIndex+1, len(rf.log))
			return
		}
		i := 0
		conflictIndex := 0
		for ; i < len(args.Entries); i++ {
			if len(rf.log) <= args.PrevLogIndex+i {
				break
			}
			if rf.log[args.PrevLogIndex+i].Term != args.Entries[i].Term {
				conflictIndex = args.PrevLogIndex + i + 1
				DPrintf("server %d log conflictIndex = %d  with leader args = %+v server log = %v committed = %d\n", rf.me, conflictIndex, args, rf.log, rf.committedIndex)
				break
			}
		}
		if conflictIndex > 0 {
			if conflictIndex <= rf.committedIndex {
				reply.Success = false
				reply.Term = rf.currentTerm
				reply.ConflictIndex = conflictIndex
				return
			} else {
				rf.log = rf.log[:conflictIndex-1]
			}
		}
		for ; i < len(args.Entries); i++ {
			rf.log = append(rf.log, args.Entries[i])
		}
		oldCommitted := rf.committedIndex
		if args.LeaderCommitted > rf.committedIndex {
			rf.committedIndex = Min(args.LeaderCommitted, len(rf.log))
		}
		if oldCommitted != rf.committedIndex {
			rf.persist()
			go func() {
				rf.machineCh <- 1
			}()
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
