package raft

import "fmt"

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else if rf.currentTerm == args.Term && rf.logIsNewerOrEqual(args.LastCommittingLogTerm, args.LastCommittingLogIndex) {
		if rf.state == LEADER {
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
		} else if rf.state == CANDIDATE {
			if ok := rf.vote(args.CandidateId); ok {
				rf.state = FOLLOWER
				reply.Term = rf.currentTerm
				reply.VoteGranted = true
			} else {
				reply.Term = rf.currentTerm
				reply.VoteGranted = false
			}
		} else {
			if ok := rf.vote(args.CandidateId); ok {
				reply.Term = rf.currentTerm
				reply.VoteGranted = true
			} else {
				reply.Term = rf.currentTerm
				reply.VoteGranted = false
			}
		}
	} else if rf.currentTerm < args.Term && rf.logIsNewerOrEqual(args.LastCommittingLogTerm, args.LastCommittingLogIndex) {
		rf.updateCurrentTerm(args.Term)
		rf.state = FOLLOWER
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
	reply.LastCommittingLogIndex = rf.committingIndex
	if reply.LastCommittingLogIndex > 0 {
		reply.LastCommittingLogTerm = rf.log[reply.LastCommittingLogIndex-1].Term
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) RequestAppendEntries(args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm > args.Term {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.FollowerCommitting = rf.committingIndex
		return
	} else {
		if rf.state == LEADER && rf.currentTerm == args.Term {
			panic(fmt.Errorf("there have two leaders %d %d in term %d", rf.me, args.LeaderId, args.Term))
		}
		rf.state = FOLLOWER
		rf.updateCurrentTerm(args.Term)
		go func() {
			rf.heartBeat <- 1
		}()
		if args.PrevLogIndex > len(rf.log) || args.PrevLogIndex > 0 && rf.log[args.PrevLogIndex-1].Term != args.PrevLogTerm {
			reply.Success = false
			reply.Term = rf.currentTerm
			reply.FollowerCommitting = rf.committingIndex
			reply.ConflictIndex = Min(len(rf.log), args.PrevLogIndex)
			rf.committingIndex = rf.committedIndex
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
				DPrintf("server %d log conflictIndex = %d  with leader args = %+v server log = %v committing = %d committed = %d\n", rf.me, conflictIndex, args, rf.log, rf.committingIndex, rf.committedIndex)
				break
			}
		}
		if conflictIndex > 0 {
			rf.committingIndex = rf.committedIndex
			if conflictIndex <= rf.committedIndex {
				reply.Success = false
				reply.Term = rf.currentTerm
				reply.FollowerCommitting = rf.committingIndex
				reply.ConflictIndex = conflictIndex
				return
			} else {
				rf.log = rf.log[:conflictIndex-1]
				rf.persist()
			}
		}
		for ; i < len(args.Entries); i++ {
			rf.log = append(rf.log, args.Entries[i])
		}
		oldCommitting := rf.committingIndex
		oldCommitted := rf.committedIndex
		if args.LeaderCommitting > rf.committingIndex {
			rf.committingIndex = Min(args.LeaderCommitting, len(rf.log))
		}
		if args.LeaderCommitted > rf.committedIndex {
			rf.committedIndex = Min(args.LeaderCommitted, len(rf.log))
		}
		if oldCommitting != rf.committingIndex || oldCommitted != rf.committedIndex {
			rf.persist()
			go func() {
				rf.machineCh <- 1
			}()
		}
		reply.Success = true
		reply.Term = rf.currentTerm
		reply.FollowerCommitting = rf.committingIndex
		reply.ConflictIndex = 0
	}
}

func (rf *Raft) sendRequestAppendEntries(server int, args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.RequestAppendEntries", args, reply)
	if ok {
		DPrintf("LEADER %d send sever %d RequestAppendEntries ok = %v args = %+v reply = %+v\n", rf.me, server, ok, args, reply)
	}
	return ok
}
