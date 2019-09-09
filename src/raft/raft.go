package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"context"
	"fmt"
	"labgob"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	machineCh chan int
	state     int      // raft state FOLLOWER，CANDIDATE，LEADER
	heartBeat chan int // heartbeat chan used by FOLLOWER
	ctx       context.Context
	close     context.CancelFunc
	// Persistent state on all servers
	currentTerm int     // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int     // candidateId that received vote in current term (or null if none)
	log         []Entry // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
	// Volatile state on all servers:
	committingIndex int // index of highest log entry known to be committing (initialized to 0, increases monotonically)
	committedIndex  int // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied     int // index of highest log entry applied to state machine (initialized to 0, increases monotonically)
	// Volatile state on leaders: (Reinitialized after election)
	nextIndex        []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex       []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
	peersCommitIndex []int
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.state = FOLLOWER
	rf.heartBeat = make(chan int, 10)
	rf.machineCh = make(chan int, 200)
	ctx, cancel := context.WithCancel(context.Background())
	rf.close = cancel
	rf.ctx = ctx
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.stateMachine(rf.ctx, applyCh)
	go rf.doFollower()
	return rf
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != LEADER {
		return 0, 0, false
	} else {
		entry := Entry{
			Term:    rf.currentTerm,
			Index:   len(rf.log),
			Command: command,
		}
		rf.log = append(rf.log, entry)
		rf.nextIndex[rf.me] = len(rf.log) + 1
		rf.matchIndex[rf.me] = len(rf.log)
		go rf.sendAllHeartbeat()
		DPrintf("---------------------leader %d (term %d) start command %v and committingIndex = %d committedIndex = %d matchIndex= %v nextIndex = %v  log = %v\n", rf.me, rf.currentTerm, command, rf.committingIndex, rf.committedIndex, rf.matchIndex, rf.nextIndex, rf.log)
		return rf.matchIndex[rf.me], rf.currentTerm, true
	}
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.mu.Lock()
	rf.state = STOPED
	rf.mu.Unlock()
	rf.close()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == LEADER
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.committingIndex)
	e.Encode(rf.committedIndex)
	e.Encode(rf.lastApplied)
	e.Encode(rf.log[:rf.committingIndex])
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		rf.votedFor = -1
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.committingIndex)
	d.Decode(&rf.committedIndex)
	d.Decode(&rf.lastApplied)
	d.Decode(&rf.log)
	DPrintf("server %d init params currentTerm = %d votedFor = %d committingIndex = %d committedIndex= %d lastApplied = %d log= %v", rf.me, rf.currentTerm, rf.votedFor, rf.committingIndex, rf.committedIndex, rf.lastApplied, rf.log)
}

// need lock during call this func
func (rf *Raft) vote(server int) bool {
	if rf.votedFor == -1 {
		rf.persist()
		rf.votedFor = server
		return true
	} else if rf.votedFor == server {
		return true
	} else {
		return false
	}
}

// need lock during call this func
func (rf *Raft) logIsNewerOrEqual(lastCommittingLogTerm, lastCommittingLogIndex int) bool {
	myLastCommittingLogIndex := rf.committingIndex
	myLastCommittingLogTerm := 0
	if rf.committingIndex > 0 {
		myLastCommittingLogTerm = rf.log[rf.committingIndex-1].Term
	}
	return myLastCommittingLogTerm < lastCommittingLogTerm || myLastCommittingLogTerm == lastCommittingLogTerm && myLastCommittingLogIndex <= lastCommittingLogIndex
}

// need lock during call this func
func (rf *Raft) updateCurrentTerm(term int) {
	if rf.currentTerm < term {
		rf.votedFor = -1
		rf.currentTerm = term
	}
}

// used by leader
func (rf *Raft) updateCommitted() {
	for {
		c := 0
		index := rf.committedIndex + 1
		for i := 0; i < len(rf.peers); i++ {
			if rf.peersCommitIndex[i] >= index {
				c++
			}
		}
		if c >= len(rf.peers)/2+1 {
			rf.committedIndex = index
			rf.persist()
			DPrintf("LEADER %d (term %d) committedIndex = %d peersCommitIndex= %v log = %v\n", rf.me, rf.currentTerm, rf.committedIndex, rf.peersCommitIndex, rf.log)
			rf.machineCh <- 1
		} else {
			break
		}
	}

}

// used by leader
func (rf *Raft) updateCommitting() {
	for {
		c := 0
		index := rf.committingIndex + 1
		for i := 0; i < len(rf.peers); i++ {
			if rf.matchIndex[i] >= index {
				c++
			}
		}
		if c >= len(rf.peers)/2+1 {
			rf.committingIndex = index
			rf.peersCommitIndex[rf.me] = index
			rf.persist()
			DPrintf("LEADER %d (term %d) committingIndex = %d matchIndex= %v log = %v\n", rf.me, rf.currentTerm, rf.committingIndex, rf.matchIndex, rf.log)
		} else {
			break
		}
	}

}

func (rf *Raft) replicateLog(server int) {
	rf.mu.Lock()
	if rf.state != LEADER {
		rf.mu.Unlock()
		return
	}
	var prevLogIndex, prevLogTerm = 0, 0
	if rf.nextIndex[server] > 1 {
		prevLogIndex = rf.nextIndex[server] - 1
		prevLogTerm = rf.log[prevLogIndex-1].Term
	}
	args := RequestAppendEntriesArgs{
		Term:             rf.currentTerm,
		LeaderId:         rf.me,
		PrevLogIndex:     prevLogIndex,
		PrevLogTerm:      prevLogTerm,
		LeaderCommitting: rf.committingIndex,
		LeaderCommitted:  rf.committedIndex,
	}
	if rf.nextIndex[server]-1 < len(rf.log) {
		args.Entries = make([]Entry, len(rf.log[rf.nextIndex[server]-1:]))
		// must copy or occur data race
		copy(args.Entries, rf.log[rf.nextIndex[server]-1:])
	}
	rf.mu.Unlock()
	reply := RequestAppendEntriesReply{}
	if ok := rf.sendRequestAppendEntries(server, &args, &reply); ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.state != LEADER {
			return
		}
		if reply.Term > rf.currentTerm {
			rf.state = FOLLOWER
			rf.updateCurrentTerm(reply.Term)
		} else {
			if reply.Success {
				if rf.nextIndex[server] < args.PrevLogIndex+len(args.Entries)+1 {
					rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
					rf.matchIndex[server] = rf.nextIndex[server] - 1
				}
				if reply.FollowerCommitting > rf.peersCommitIndex[server] {
					rf.peersCommitIndex[server] = reply.FollowerCommitting
				}
				rf.updateCommitting()
				rf.updateCommitted()
			} else {
				rf.nextIndex[server] = Max(1, Min(reply.ConflictIndex-1, len(rf.log)))
				if rf.matchIndex[server] >= rf.nextIndex[server] {
					rf.matchIndex[server] = rf.nextIndex[server] - 1
				}
			}
		}
	}
}

func (rf *Raft) sendAllHeartbeat() {
	for i := 0; i < len(rf.peers); i++ {
		if rf.me == i {
			continue
		}
		go rf.replicateLog(i)
	}
}

func (rf *Raft) doElection(ctx context.Context) (succ bool, err error) {
	rf.mu.Lock()
	if rf.state != CANDIDATE {
		rf.mu.Unlock()
		return false, fmt.Errorf("CANDIDATE %d election failed now state is %s\n", rf.me, stateMap[rf.state])
	}
	if ok := rf.vote(rf.me); !ok {
		rf.mu.Unlock()
		return false, nil
	}
	var voteTerm = rf.currentTerm
	args := RequestVoteArgs{
		Term:                   rf.currentTerm,
		CandidateId:            rf.me,
		LastCommittingLogIndex: rf.committingIndex,
	}
	if args.LastCommittingLogIndex > 0 {
		args.LastCommittingLogTerm = rf.log[args.LastCommittingLogIndex-1].Term
	}
	rf.mu.Unlock()
	ch := make(chan *RequestVoteReply, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			// mock vote self reply
			go func() {
				ch <- &RequestVoteReply{
					Term:        voteTerm,
					VoteGranted: true,
				}
			}()
		} else {
			go func(server int) {
				reply := RequestVoteReply{}
				if ok := rf.sendRequestVote(server, &args, &reply); ok {
					DPrintf("CANDIDATE %d requestVote client %d args %+v  reply %+v\n", rf.me, server, args, reply)
					ch <- &reply
				} else {
					//DPrintf("CANDIDATE %d requestVote client %d failed\n", rf.me, server)
					ch <- &reply
				}
			}(i)
		}
	}
	replyCount := 0
	voteCount := 0
	for i := 0; i < len(rf.peers); i++ {
		// check if state switch in waiting vote
		rf.mu.Lock()
		if rf.state != CANDIDATE {
			rf.mu.Unlock()
			return false, fmt.Errorf("CANDIDATE %d election failed now state is %s", rf.me, stateMap[rf.state])
		} else {
			rf.mu.Unlock()
		}
		select {
		case <-ctx.Done():
			if replyCount >= len(rf.peers)/2+1 {
				return false, nil
			} else {
				return false, fmt.Errorf("CANDIDATE %d election timeout and lose majority peers in term %d", rf.me, voteTerm)
			}
		case reply := <-ch:
			if reply.Term <= 0 {
				continue
			} else {
				replyCount++
				if reply.VoteGranted == true && reply.Term == voteTerm {
					voteCount++
				} else if reply.VoteGranted == false {
					rf.mu.Lock()
					if rf.logIsNewerOrEqual(reply.LastCommittingLogTerm, reply.LastCommittingLogIndex) {
						rf.state = FOLLOWER
						rf.updateCurrentTerm(reply.Term)
						rf.mu.Unlock()
						return false, fmt.Errorf("CANDIDATE %d election failed one of peers has newer log\n", rf.me)
					} else {
						rf.mu.Unlock()
						return false, nil
					}
				}
			}
		}
		if voteCount >= len(rf.peers)/2+1 {
			rf.mu.Lock()
			if rf.state == CANDIDATE && rf.currentTerm == voteTerm {
				rf.initLeader()
				DPrintf("CANDIDATE %d win election in term %d  rf = %+v \n", rf.me, rf.currentTerm, rf)
				rf.mu.Unlock()
				return true, nil
			} else {
				rf.mu.Unlock()
				return false, fmt.Errorf("CANDIDATE %d win election in term %d but currentTerm or state is changed\n", rf.me, voteTerm)
			}
		}
	}
	return false, nil
}

func (rf *Raft) initLeader() {
	rf.state = LEADER
	if rf.committingIndex > 1 {
		rf.log = rf.log[:rf.committingIndex]
	}
	rf.matchIndex = make([]int, len(rf.peers))
	rf.nextIndex = make([]int, len(rf.peers))
	rf.peersCommitIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.log) + 1
	}
	rf.matchIndex[rf.me] = len(rf.log)
	rf.peersCommitIndex[rf.me] = rf.committingIndex
}

func (rf *Raft) doFollower() {
	t := time.NewTimer(HEARTBEATTIMEOUT)
	for {
		t.Reset(HEARTBEATTIMEOUT)
		select {
		case <-rf.ctx.Done():
			DPrintf("FOLLOWER %d closed\n", rf.me)
			return
		case <-rf.heartBeat:
		case <-t.C:
			DPrintf("FOLLOWER %d heartbeat signal timeout\n", rf.me)
			rf.mu.Lock()
			rf.state = CANDIDATE
			rf.mu.Unlock()
			go rf.doCandidate()
			return
		}
	}
}

func (rf *Raft) doCandidate() {
	rf.mu.Lock()
	rf.updateCurrentTerm(rf.currentTerm + 1)
	rf.mu.Unlock()
	t := time.NewTimer(time.Duration(rand.Int63n(100)) * HEARTBEATTIMEOUT / 100)
	for {
		rand.Seed(time.Now().UnixNano())
		t.Reset(time.Duration(rand.Int63n(100)) * HEARTBEATTIMEOUT / 100)
		select {
		case <-rf.ctx.Done():
			DPrintf("CANDIDATE %d closed\n", rf.me)
			return
		case <-t.C:
			rf.mu.Lock()
			if rf.state == FOLLOWER {
				rf.mu.Unlock()
				go rf.doFollower()
				return
			} else {
				rf.mu.Unlock()
			}
			ctx, _ := context.WithTimeout(context.Background(), ELECTIONTIMEOUT)
			if succ, err := rf.doElection(ctx); succ {
				go rf.doLeader()
				return
			} else {
				if err == nil {
					rf.mu.Lock()
					if rf.state == CANDIDATE {
						rf.updateCurrentTerm(rf.currentTerm + 1)
					}
					rf.mu.Unlock()
				} else {
					DPrintf("%v\n", err)
				}
			}
		}
	}
}

func (rf *Raft) doLeader() {
	t := time.NewTicker(HEARTBEAT)
	rf.sendAllHeartbeat()
	for {
		select {
		case <-rf.ctx.Done():
			DPrintf("LEADER %d closed\n", rf.me)
			return
		case <-t.C:
			rf.sendAllHeartbeat()
		}
		rf.mu.Lock()
		if rf.state == FOLLOWER {
			rf.mu.Unlock()
			go rf.doFollower()
			return
		} else {
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) stateMachine(ctx context.Context, executeCh chan ApplyMsg) {
	for {
		select {
		case <-ctx.Done():
			DPrintf("server stateMachine %d closed \n", rf.me)
			return
		case <-rf.machineCh:
			var executeCommands []Entry
			var baseIndex int
			rf.mu.Lock()
			if rf.lastApplied < rf.committedIndex {
				executeCommands = rf.log[rf.lastApplied:rf.committedIndex]
				baseIndex = rf.lastApplied
			}
			rf.mu.Unlock()
			for i, c := range executeCommands {
				executeCh <- ApplyMsg{
					CommandValid: true,
					CommandIndex: baseIndex + i + 1,
					Command:      c.Command,
				}
			}
			if len(executeCommands) > 0 {
				DPrintf("server %d stateMachine success execute commands %+v\n", rf.me, executeCommands)
				rf.mu.Lock()
				if rf.lastApplied < baseIndex+len(executeCommands) {
					rf.lastApplied = baseIndex + len(executeCommands)
					rf.persist()
				}
				rf.mu.Unlock()
			}
		}
	}
}
