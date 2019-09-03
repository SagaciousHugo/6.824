package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(Command interface{}) (index, currentTerm, isleader)
//   start agreement on a new log entry
// rf.GetState() (currentTerm, isLeader)
//   ask a Raft for its current currentTerm, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"
)
import "labrpc"

const (
	FOLLOWER = iota
	CANDIDATE
	LEADER
)

var (
	stateArray = [3]string{"FOLLOWER", "CANDIDATE", "LEADER"}
)

const (
	HEARTBEATTIMEOUT = 250 * time.Millisecond
	HEARTBEAT        = 200 * time.Millisecond
	ELECTIONTIMEOUT  = 1000 * time.Millisecond
)

// import "bytes"
// import "labgob"

//
// as each Raft peer becomes aware that successive log Entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type Entry struct {
	Term    int
	Command interface{}
}

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

	applyCh   chan ApplyMsg
	chs       [3]chan int // chan array for switch state chs[0] for FOLLOWER, chs[1] for CANDIDATE, chs[2] for LEADER
	state     int         // raft state
	heartBeat chan int    // heartbeat chan
	// Persistent state on all servers
	currentTerm int         // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    map[int]int // candidateId that received vote in current term (or null if none)
	log         []Entry     // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
	// Volatile state on all servers:
	commitIndex int // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0, increases monotonically)
	// Volatile state on leaders: (Reinitialized after election)
	nextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
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
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate’s Term
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
/*
Receiver implementation:
1. Reply false if term < currentTerm (§5.1)
2. If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
*/
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = args.Term
		reply.VoteGranted = false
	} else {
		myLastLogIndex := len(rf.log) - 1
		myLastLogTerm := 0
		if myLastLogIndex >= 0 {
			myLastLogTerm = rf.log[myLastLogIndex].Term
		}
		if myLastLogTerm < args.LastLogTerm || myLastLogTerm == args.LastLogTerm && myLastLogIndex <= args.LastLogIndex {
			rf.currentTerm = args.Term
			rf.state = FOLLOWER
			if ok := rf.voteInTerm(args.CandidateId, args.Term); ok {
				reply.Term = args.Term
				reply.VoteGranted = true
			} else {
				reply.Term = args.Term
				reply.VoteGranted = false
			}
		} else {
			reply.Term = args.Term
			reply.VoteGranted = false
		}
	}

}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next Command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// Command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the Command will appear at
// if it's ever committed. the second return value is the current
// currentTerm. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != LEADER {
		return -1, -1, false
	} else {
		en := Entry{
			Term:    rf.currentTerm,
			Command: command,
		}
		rf.log = append(rf.log, en)
		rf.nextIndex[rf.me]++
		rf.matchIndex[rf.me] = rf.nextIndex[rf.me] - 1
		//DPrintf("---------------------leader %d start command %v and commitIndex = %d matchIndex= %v nextIndex = %v  log = %v\n", rf.me, command, rf.commitIndex, rf.matchIndex, rf.nextIndex, rf.log)
		DPrintf("---------------------leader %d start command %v and commitIndex = %d matchIndex= %v nextIndex = %v \n", rf.me, command, rf.commitIndex, rf.matchIndex, rf.nextIndex)
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
	rf.votedFor = make(map[int]int, 10)
	rf.applyCh = applyCh
	// Your initialization code here (2A, 2B, 2C).
	rf.chs[0] = make(chan int)
	rf.chs[1] = make(chan int)
	rf.chs[2] = make(chan int)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.heartBeat = make(chan int, 10)
	rf.commitIndex = 0
	rf.lastApplied = 0
	go rf.doFollower()
	go rf.doCandidate()
	go rf.doLeader()
	go func() {
		rf.chs[FOLLOWER] <- 1
	}()
	return rf
}

type RequestAppendEntriesArgs struct {
	Term         int     // leader’s term
	LeaderId     int     // so follower can redirect clients
	PrevLogIndex int     // index of log entry immediately preceding new ones
	PrevLogTerm  int     // term of prevLogIndex entry
	Entries      []Entry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int     // leader’s commitIndex
}

type RequestAppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

/*
Receiver implementation:
1. Reply false if term < currentTerm (§5.1)
2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
4. Append any new entries not already in the log
5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
*/
func (rf *Raft) RequestAppendEntries(args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// check if need switch state
	if rf.state == CANDIDATE && args.Term >= rf.currentTerm {
		rf.state = FOLLOWER
		rf.currentTerm = args.Term
		DPrintf("server %d (CANDIDATE) find %d leader term equal or higher and switch to FOLLOWER \n", rf.me, args.LeaderId)
	} else if rf.state == LEADER && args.Term > rf.currentTerm {
		rf.state = FOLLOWER
		rf.currentTerm = args.Term
		DPrintf("server %d (LEADER) find %d leader term higher and switch to FOLLOWER \n", rf.me, args.LeaderId)
	}
	if rf.state == FOLLOWER && args.Term >= rf.currentTerm {
		rf.heartBeat <- 1
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm

		if args.PrevLogIndex == 0 || args.Term == 0 {
			reply.Success = true
			rf.log = args.Entries
		} else if len(rf.log) >= args.PrevLogIndex {
			if rf.log[args.PrevLogIndex-1].Term == args.PrevLogTerm {
				reply.Success = true
				if len(args.Entries) > 0 {
					rf.log = append(rf.log[:args.PrevLogIndex], args.Entries...)
				}
			} else {
				reply.Success = false
				rf.log = rf.log[:args.PrevLogIndex]
			}
		} else {
			reply.Success = false
		}
	} else {
		reply.Success = false
		reply.Term = rf.currentTerm
	}
	if reply.Success && rf.commitIndex < args.LeaderCommit {
		if args.LeaderCommit > len(rf.log) {
			rf.commitIndex = len(rf.log)
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		//DPrintf("server %d commitIndex = %d log = %v\n", rf.me, rf.commitIndex, rf.log)
	}
}

func (rf *Raft) sendRequestAppendEntries(server int, args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.RequestAppendEntries", args, reply)
	return ok
}

/*
Followers (§5.2):
• Respond to RPCs from candidates and leaders
• If election timeout elapses without receiving AppendEntries RPC from current leader or granting vote to candidate: convert to candidate

All Servers:
• If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine (§5.3)
• If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
*/
func (rf *Raft) doFollower() {
Start:
	<-rf.chs[FOLLOWER]
	t := time.NewTimer(HEARTBEATTIMEOUT)
	for {
		t.Reset(HEARTBEATTIMEOUT)
		select {
		case <-rf.heartBeat:
			//DPrintf("server %d 收到心跳\n", rf.me)
			rf.checkLastApplied()
		case <-t.C:
			DPrintf("server %d 心跳信号超时\n", rf.me)
			rf.updateState(FOLLOWER, CANDIDATE)
			rf.chs[CANDIDATE] <- 1
			goto Start
		}
	}
}

/*
Candidates (§5.2):
• On conversion to candidate, start election:
• Increment currentTerm
• Vote for self
• Reset election timer
• Send RequestVote RPCs to all other servers
• If votes received from majority of servers: become leader
• If AppendEntries RPC received from new leader: convert to follower
• If election timeout elapses: start new election

All Servers:
• If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine (§5.3)
• If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
*/
func (rf *Raft) doCandidate() {
Start:
	<-rf.chs[CANDIDATE]
	newTerm := rf.incrementCurrentTerm(CANDIDATE)
	for {
		rand.Seed(time.Now().UnixNano())
		d := HEARTBEAT + time.Duration(rand.Int63n(100))*HEARTBEATTIMEOUT/100
		time.Sleep(d)
		if ok := rf.checkState(FOLLOWER); ok {
			rf.chs[FOLLOWER] <- 1
			goto Start
		}
		if ok := rf.voteSelfInTerm(newTerm); !ok {
			continue
		} else {
			ctx, _ := context.WithTimeout(context.Background(), ELECTIONTIMEOUT)
			if success, err := rf.runForElection(ctx, newTerm); success && err == nil {
				rf.chs[LEADER] <- 1
				goto Start
			} else if err == nil {
				newTerm = rf.incrementCurrentTerm(CANDIDATE)
			}
		}
	}
}

func (rf *Raft) runForElection(ctx context.Context, term int) (success bool, err error) {
	_, _, _, _, _, log := rf.getAllState()
	// build request vote args
	var lastLogTerm = 0
	var lastLogIndex = len(log)
	if lastLogIndex > 0 {
		lastLogTerm = log[lastLogIndex-1].Term
	}
	ch := make(chan *RequestVoteReply, len(rf.peers))
	args := RequestVoteArgs{
		Term:         term,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	// request vote
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			// mock vote self request
			go func() {
				ch <- &RequestVoteReply{
					Term:        term,
					VoteGranted: true,
				}
			}()
		} else {
			go func(server int) {
				reply := RequestVoteReply{}
				if ok := rf.sendRequestVote(server, &args, &reply); ok {
					DPrintf("server %d -> client %d request %+v  reply %+v\n", rf.me, server, args, reply)
					ch <- &reply
				}
			}(i)
		}

	}
	// wait and check vote
	votes := 0
	replies := 0
	for i := 0; i < len(rf.peers); i++ {
		// check if state switch in waiting vote
		if ok := rf.checkState(FOLLOWER); ok {
			return false, fmt.Errorf("server %d election failed now state is follower\n", rf.me)
		}
		select {
		case <-ctx.Done():
			if replies >= rf.majorityCount() {
				return false, nil
			} else {
				return false, fmt.Errorf("server %d election timeout and lose majority peers in term %d\n", rf.me, term)
			}
		case reply := <-ch:
			replies++
			if reply.VoteGranted == true && reply.Term == term {
				votes++
			} else if reply.VoteGranted == false && reply.Term > term {
				rf.updateTermAndState(reply.Term, CANDIDATE, FOLLOWER)
				return false, fmt.Errorf("server %d election failed one of peers has higher term\n", rf.me)
			}
		}
		if votes >= rf.majorityCount() {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.state == CANDIDATE && rf.currentTerm == term {
				rf.state = LEADER
				rf.matchIndex = make([]int, len(rf.peers))
				rf.nextIndex = make([]int, len(rf.peers))
				for i := 0; i < len(rf.peers); i++ {
					rf.nextIndex[i] = len(rf.log) + 1
				}
				return true, nil
			} else {
				return false, fmt.Errorf("server %d election switch to leader failed, currentTerm or state is changed\n", rf.me)
			}
		}
	}
	DPrintf("server %d not have enough vote (%d) in term %d\n", rf.me, votes, term)
	return false, nil
}

/*
Leaders:
Upon election: send initial empty AppendEntries RPCs (heartbeat) to each server; repeat during idle periods to prevent election timeouts (§5.2)
• If command received from client: append entry to local log, respond after entry applied to state machine (§5.3)
• If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
• If successful: update nextIndex and matchIndex for follower (§5.3)
• If AppendEntries fails because of log inconsistency: decrement nextIndex and retry (§5.3)
• If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4).

All Servers:
• If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine (§5.3)
• If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
*/
func (rf *Raft) doLeader() {
Start:
	<-rf.chs[LEADER]
	t := time.NewTicker(HEARTBEAT)
	rf.sendAllHeartbeat()
	ch := make(chan int)
	for {
		if ok := rf.checkState(FOLLOWER); ok {
			rf.chs[FOLLOWER] <- 1
			goto Start
		}
		select {
		case <-t.C:
			currentTerm, _, commitIndex, _, nextIndex, log := rf.getAllState()
			for i := 0; i < len(rf.peers); i++ {
				if rf.me == i {
					continue
				}
				go rf.replicateLog(i, currentTerm, commitIndex, nextIndex, log, ch)
			}
		case retryServer := <-ch:
			currentTerm, _, commitIndex, _, nextIndex, log := rf.getAllState()
			go rf.replicateLog(retryServer, currentTerm, commitIndex, nextIndex, log, ch)
		}

		rf.checkCommitIndex()
		rf.checkLastApplied()
	}
}

func (rf *Raft) updateTerm(newTerm int) bool {
	if rf.currentTerm < newTerm {
		rf.currentTerm = newTerm
		return true
	} else {
		return false
	}
}

func (rf *Raft) updateState(prev int, next int) (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == prev {
		rf.state = next
		return rf.state, true
	} else {
		return rf.state, false
	}
}

func (rf *Raft) updateTermAndState(newTerm, prev, next int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm < newTerm && rf.state == prev {
		rf.currentTerm = newTerm
		rf.state = next
		return true
	} else {
		return false
	}
}

func (rf *Raft) updateStateInTerm(newTerm, prev, next int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm == newTerm && rf.state == prev {
		rf.state = next
		return true
	} else {
		return false
	}
}

func (rf *Raft) voteInTerm(server, term int) bool {
	if rf.currentTerm != term {
		return false
	} else {
		if v, ok := rf.votedFor[term]; !ok {
			rf.votedFor[term] = server
			if term > 10 {
				delete(rf.votedFor, term-10)
			}
			return true
		} else if v == rf.me {
			return true
		} else {
			return false
		}
	}
}

func (rf *Raft) voteSelfInTerm(term int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.voteInTerm(rf.me, term)
}

// Only used by leader
func (rf *Raft) checkCommitIndex() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for {
		c := 0
		newCommitIndex := rf.commitIndex + 1

		for i := 0; i < len(rf.peers); i++ {
			if rf.matchIndex[i] >= newCommitIndex {
				c++
			}
		}
		if c >= rf.majorityCount() {
			rf.commitIndex = newCommitIndex
			DPrintf("server %d commitIndex = %d log = %v\n", rf.me, rf.commitIndex, rf.log)
		} else {
			break
		}
	}
}

// used by leader and follower
func (rf *Raft) checkLastApplied() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for {
		if rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
		} else {
			break
		}
		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[rf.lastApplied-1].Command,
			CommandIndex: rf.lastApplied,
		}
		rf.applyCh <- msg
		//DPrintf("server %d send %v to applyCh\n", rf.me, msg)
		//TODO 持久化

	}
}

func (rf *Raft) sendAllHeartbeat() {
	currentTerm, _, commitIndex, _, nextIndex, log := rf.getAllState()
	for i := 0; i < len(rf.peers); i++ {
		if rf.me == i {
			continue
		}
		go rf.sendHeartbeat(i, currentTerm, commitIndex, nextIndex, log)
	}
}

func (rf *Raft) sendHeartbeat(server, currentTerm, commitIndex int, nextIndex []int, log []Entry) {
	var prevLogIndex, prevLogTerm int
	if nextIndex[server] > 1 {
		prevLogIndex = nextIndex[server] - 1
		prevLogTerm = log[prevLogIndex-1].Term
	}
	args := RequestAppendEntriesArgs{
		Term:         currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		LeaderCommit: commitIndex,
	}
	reply := RequestAppendEntriesReply{}
	if ok := rf.sendRequestAppendEntries(server, &args, &reply); ok {
		if !reply.Success {
			rf.mu.Lock()
			rf.nextIndex[server]--
			rf.mu.Unlock()
		}
		//DPrintf("%d server %d heartbeat with server %d failed\n", rf.me, server, t.UnixNano()/1e6)
	} else {
		//DPrintf("%d server %d heartbeat with server %d failed\n", rf.me, server, t.UnixNano()/1e6)
	}

}

func (rf *Raft) replicateLog(server, currentTerm, commitIndex int, nextIndex []int, log []Entry, ch chan int) {
	var prevLogIndex, prevLogTerm, logLength = 0, 0, len(log)
	if nextIndex[server] > 1 {
		prevLogIndex = nextIndex[server] - 1
		prevLogTerm = log[prevLogIndex-1].Term
	}
	args := RequestAppendEntriesArgs{
		Term:         currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		LeaderCommit: commitIndex,
	}
	if nextIndex[server]-1 < logLength {
		args.Entries = log[nextIndex[server]-1 : logLength]
	}
	reply := RequestAppendEntriesReply{}
	if ok := rf.sendRequestAppendEntries(server, &args, &reply); ok {
		if reply.Term > currentTerm {
			rf.updateStateInTerm(currentTerm, LEADER, FOLLOWER)
		} else {
			if reply.Success {
				rf.mu.Lock()
				rf.nextIndex[server] += len(args.Entries)
				rf.matchIndex[server] = rf.nextIndex[server] - 1
				rf.mu.Unlock()
			} else {
				rf.mu.Lock()
				step := 1
				if (rf.nextIndex[server]-rf.matchIndex[server])/2 > 1 {
					step = (rf.nextIndex[server] - rf.matchIndex[server]) / 2
				}
				rf.nextIndex[server] -= step
				rf.mu.Unlock()
				ch <- server
			}
		}
		//DPrintf("leader %d send to client %d request %+v reply %+v matchIndex = %v nextIndex = %v\n", rf.me, server, args, reply, rf.matchIndex, rf.nextIndex)

	} else {
		//DPrintf("%d server %d heartbeat with server %d failed\n", rf.me, server, t.UnixNano()/1e6)
	}
}

func (rf *Raft) getAllState() (currentTerm, state, commitIndex int, matchIndex []int, nextIndex []int, log []Entry) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state, rf.commitIndex, rf.matchIndex, rf.nextIndex, rf.log
}

func (rf *Raft) checkState(expected int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state == expected
}

func (rf *Raft) majorityCount() int {
	return len(rf.peers)/2 + 1
}

func (rf *Raft) incrementCurrentTerm(state ...int) (newTerm int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if len(state) == 0 || len(state) > 0 && rf.state == state[0] {
		rf.currentTerm++
	}
	return rf.currentTerm
}
