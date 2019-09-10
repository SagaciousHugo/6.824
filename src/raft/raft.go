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
	committedIndex int // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied    int // index of highest log entry applied to state machine (initialized to 0, increases monotonically)
	// Volatile state on leaders: (Reinitialized after election)
	nextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
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
	rf.heartBeat = make(chan int, 10)
	rf.machineCh = make(chan int, 200)
	ctx, cancel := context.WithCancel(context.Background())
	rf.close = cancel
	rf.ctx = ctx
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.state = FOLLOWER
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
			Index:   len(rf.log) + 1,
			Command: command,
		}
		rf.log = append(rf.log, entry)
		rf.nextIndex[rf.me] = len(rf.log) + 1
		rf.matchIndex[rf.me] = len(rf.log)
		go rf.sendReplicateLogToAll()
		DPrintf("---------------------leader %d (term %d) start command %v and committedIndex = %d matchIndex= %v nextIndex = %v  log = %v\n", rf.me, rf.currentTerm, command, rf.committedIndex, rf.matchIndex, rf.nextIndex, rf.log)
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
	e.Encode(rf.committedIndex)
	e.Encode(rf.lastApplied)
	e.Encode(rf.log)
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
	d.Decode(&rf.committedIndex)
	d.Decode(&rf.lastApplied)
	d.Decode(&rf.log)
	DPrintf("server %d init params currentTerm = %d votedFor = %d  committedIndex= %d lastApplied = %d log= %v", rf.me, rf.currentTerm, rf.votedFor, rf.committedIndex, rf.lastApplied, rf.log)
}

// need lock during call this func
func (rf *Raft) vote(server int) bool {
	if rf.votedFor == -1 {
		rf.votedFor = server
		rf.persist()
		return true
	} else if rf.votedFor == server {
		return true
	} else {
		return false
	}
}

// need lock during call this func
func (rf *Raft) replyLogIsNewerOrEqual(lastLogTerm, lastLogIndex int) bool {
	myLastLogIndex := len(rf.log)
	myLastLogTerm := 0
	if myLastLogIndex > 0 {
		myLastLogTerm = rf.log[myLastLogIndex-1].Term
	}
	return myLastLogTerm < lastLogTerm || myLastLogTerm == lastLogTerm && myLastLogIndex <= lastLogIndex
}

// need lock during call this func
func (rf *Raft) updateCurrentTerm(term int) {
	if rf.currentTerm < term {
		rf.votedFor = -1
		rf.currentTerm = term
		rf.persist()
	}
}

// used by leader
func (rf *Raft) updateCommitted() {
	index := rf.committedIndex
	for {
		c := 0
		index++
		for i := 0; i < len(rf.peers); i++ {
			if rf.matchIndex[i] >= index {
				c++
			}
		}
		if c >= len(rf.peers)/2+1 {
			if rf.log[index-1].Term == rf.currentTerm {
				rf.committedIndex = index
				rf.persist()
				go func() {
					rf.machineCh <- 1
				}()
				DPrintf("LEADER %d (term %d) committedIndex = %d matchIndex= %v log = %v\n", rf.me, rf.currentTerm, rf.committedIndex, rf.matchIndex, rf.log)
			} else {
				continue
			}
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
		Term:            rf.currentTerm,
		LeaderId:        rf.me,
		PrevLogIndex:    prevLogIndex,
		PrevLogTerm:     prevLogTerm,
		LeaderCommitted: rf.committedIndex,
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

func (rf *Raft) sendReplicateLogToAll() {
	for i := 0; i < len(rf.peers); i++ {
		if rf.me == i {
			continue
		}
		go rf.replicateLog(i)
	}
}

func (rf *Raft) doElection() bool {
	rf.mu.Lock()
	if rf.state != CANDIDATE {
		rf.mu.Unlock()
		DPrintf("CANDIDATE %d election failed now state is %s\n", rf.me, stateMap[rf.state])
		return false
	}
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()
	var voteTerm = rf.currentTerm
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log),
	}
	if args.LastLogIndex > 0 {
		args.LastLogTerm = rf.log[args.LastLogIndex-1].Term
	}
	rf.mu.Unlock()
	replyCh := make(chan *RequestVoteReply, len(rf.peers))
	ctx, cancel := context.WithTimeout(context.Background(), ELECTIONTIMEOUT)
	defer cancel()
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			// mock vote self reply
			go func() {
				replyCh <- &RequestVoteReply{
					Term:        voteTerm,
					VoteGranted: true,
				}
			}()
		} else {
			go rf.sendRequestVote(ctx, i, args, replyCh)
		}
	}
	voteCount := 0
	replyCount := 0
	for voteCount < len(rf.peers)/2+1 && replyCount < len(rf.peers) {
		// check if state switch in waiting vote
		rf.mu.Lock()
		if rf.state != CANDIDATE {
			rf.mu.Unlock()
			DPrintf("CANDIDATE %d election failed now state is %s\n", rf.me, stateMap[rf.state])
			return false
		} else {
			rf.mu.Unlock()
		}
		select {
		case <-ctx.Done():
			DPrintf("CANDIDATE %d election failed timeout\n", rf.me)
			/*if replyCount < len(rf.peers)+1 {
				time.Sleep(ELECTIONTIMEOUT)
			}*/
			return false
		case reply := <-replyCh:
			replyCount++
			if reply.VoteGranted == true && reply.Term == voteTerm {
				voteCount++
			} else if reply.VoteGranted == false {
				rf.mu.Lock()
				rf.updateCurrentTerm(reply.Term)
				if rf.replyLogIsNewerOrEqual(args.LastLogTerm, args.LastLogIndex) {
					rf.state = FOLLOWER
					rf.mu.Unlock()
					return false
				} else {
					rf.mu.Unlock()
				}
			}
		}
		if voteCount >= len(rf.peers)/2+1 {
			rf.mu.Lock()
			if rf.state == CANDIDATE && rf.currentTerm == voteTerm {
				rf.initLeader()
				rf.mu.Unlock()
				DPrintf("CANDIDATE %d win election in term %d \n", rf.me, rf.currentTerm)
				return true
			} else {
				rf.mu.Unlock()
				DPrintf("CANDIDATE %d win election in term %d but currentTerm or state is changed\n", rf.me, voteTerm)
				return false
			}
		}
	}
	DPrintf("CANDIDATE %d election failed in term %d not have enough votes(received %d)\n", rf.me, voteTerm, voteCount)
	return false
}

func (rf *Raft) initLeader() {
	rf.state = LEADER
	rf.matchIndex = make([]int, len(rf.peers))
	rf.nextIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.log) + 1
	}
	rf.matchIndex[rf.me] = len(rf.log)
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
			if succ := rf.doElection(); succ {
				go rf.doLeader()
				return
			}
		}
	}
}

func (rf *Raft) doLeader() {
	t := time.NewTicker(HEARTBEAT)
	rf.sendReplicateLogToAll()
	for {
		select {
		case <-rf.ctx.Done():
			DPrintf("LEADER %d closed\n", rf.me)
			return
		case <-t.C:
			rf.sendReplicateLogToAll()
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
			DPrintf("server %d stateMachine closed \n", rf.me)
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
