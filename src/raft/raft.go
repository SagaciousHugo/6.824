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
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = args.Term
		reply.VoteGranted = false
	} else {
		myLastLogIndex := len(rf.log)
		myLastLogTerm := 0
		if myLastLogIndex > 0 {
			myLastLogTerm = rf.log[myLastLogIndex-1].Term
		}
		if args.Term > rf.currentTerm && (rf.state == CANDIDATE || rf.state == LEADER) {
			rf.state = FOLLOWER
		}
		rf.currentTerm = args.Term

		if myLastLogTerm == args.LastLogTerm && myLastLogIndex <= args.LastLogTerm || myLastLogTerm < args.LastLogTerm {
			if v, ok := rf.votedFor[args.Term]; !ok || v == args.CandidateId {
				rf.votedFor[args.Term] = v
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
		DPrintf("--------------------------start command %v and log = %v\n", command, rf.log)
		for i := 0; i < len(rf.nextIndex); i++ {
			rf.nextIndex[i]++
		}
		rf.matchIndex[rf.me]++
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

func (rf *Raft) RequestAppendEntries(args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// check if need switch state
	if (rf.state == CANDIDATE && args.Term >= rf.currentTerm) || (rf.state == LEADER && args.Term > rf.currentTerm) {
		rf.state = FOLLOWER
	}
	if rf.state == FOLLOWER && args.Term >= rf.currentTerm {
		rf.heartBeat <- 1
		rf.currentTerm = args.Term
		if len(args.Entries) > 0 {
			rf.log = append(rf.log, args.Entries...)

		}
		for rf.commitIndex < args.LeaderCommit && args.LeaderCommit >= len(rf.log) {
			rf.commitIndex++
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.commitIndex-1].Command,
				CommandIndex: rf.commitIndex,
			}
		}
		if rf.commitIndex < args.LeaderCommit && args.LeaderCommit >= len(rf.log) {
			rf.commitIndex = args.LeaderCommit
		}
		DPrintf("server %d commitIndex = %d log = %v\n", rf.me, rf.commitIndex, rf.log)
		reply.Success = true
		reply.Term = rf.currentTerm
	} else {
		reply.Success = false
		reply.Term = rf.currentTerm
	}
}

func (rf *Raft) sendRequestAppendEntries(server int, args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.RequestAppendEntries", args, reply)
	return ok
}

func (rf *Raft) doFollower() {
Start:
	<-rf.chs[FOLLOWER]
	t := time.NewTimer(HEARTBEATTIMEOUT)
	for {
		t.Reset(HEARTBEATTIMEOUT)
		select {
		case <-rf.heartBeat:
			// DPrintf("server %d 收到心跳\n", rf.me)
		case <-t.C:
			DPrintf("server %d 心跳信号超时\n", rf.me)
			rf.updateState(FOLLOWER, CANDIDATE)
			rf.chs[CANDIDATE] <- 1
			goto Start
		}
	}
}

func (rf *Raft) doCandidate() {
Start:
	<-rf.chs[CANDIDATE]
	for {
		rf.mu.Lock()
		if rf.state == FOLLOWER {
			rf.mu.Unlock()
			rf.chs[FOLLOWER] <- 1
			goto Start
		} else {
			rf.mu.Unlock()
		}
		rand.Seed(time.Now().UnixNano())
		d := HEARTBEAT + time.Duration(rand.Int63n(100))*HEARTBEATTIMEOUT/100
		DPrintf("server %d sleep %s \n", rf.me, d.String())
		time.Sleep(d)
		ctx, _ := context.WithTimeout(context.Background(), ELECTIONTIMEOUT)
		if success := rf.doElection(ctx); success {
			rf.chs[LEADER] <- 1
			goto Start
		} else {
			DPrintf("server %d 尝试竞选失败\n", rf.me)
		}
	}
}

func (rf *Raft) doElection(ctx context.Context) bool {
	// vote self
	newTerm, ok := rf.voteSelfAndIncrementTerm()
	if !ok {
		return false
	}
	_, _, _, _, _, log := rf.getAllState()
	// build request vote args
	var lastLogTerm = 0
	var lastLogIndex = len(log)
	if lastLogIndex > 0 {
		lastLogTerm = log[lastLogIndex-1].Term
	}
	ch := make(chan *RequestVoteReply, len(rf.peers))
	args := RequestVoteArgs{
		Term:         newTerm,
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
					Term:        newTerm,
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
	needVotes := len(rf.peers)/2 + 1
	votes := 0
	for i := 0; i < len(rf.peers); i++ {
		// check if state switch in waiting vote
		rf.mu.Lock()
		if rf.state == FOLLOWER {
			rf.mu.Unlock()
			return false
		} else {
			rf.mu.Unlock()
		}
		select {
		case <-ctx.Done():
			DPrintf("server %d election timeout in term %d %v\n", rf.me, newTerm, ctx.Err())
			return false
		case reply := <-ch:
			if reply.VoteGranted == true && reply.Term == newTerm {
				votes++
			} else if reply.VoteGranted == false && reply.Term > newTerm {
				rf.updateStateInTerm(reply.Term, CANDIDATE, FOLLOWER)
				return false
			}
		}
		if votes >= needVotes {
			return rf.updateStateInTerm(newTerm, CANDIDATE, LEADER)
		}
	}
	DPrintf("server %d not have enough vote (%d) in term %d\n", rf.me, votes, newTerm)
	return false
}

func (rf *Raft) doLeader() {
Start:
	<-rf.chs[LEADER]
	t := time.NewTicker(HEARTBEAT)
	for {
		<-t.C
		rf.updateCommit()
		currentTerm, state, commitIndex, matchIndex, _, log := rf.getAllState()
		if state == FOLLOWER {
			rf.chs[FOLLOWER] <- 1
			goto Start
		}
		for i := 0; i < len(rf.peers); i++ {
			if matchIndex[i] < len(log) {
				prevLogIndex := matchIndex[i] - 1
				prevLogTerm := -1
				/*if prevLogIndex >= 0 {
					prevLogTerm = log[matchIndex[i]].Term
				}*/
				start := matchIndex[i]
				end := len(log)
				go func(server, prevLogIndex, prevLogTerm, commitIndex, start, end int, log []Entry) {
					args := RequestAppendEntriesArgs{
						Term:         currentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: prevLogIndex,
						PrevLogTerm:  prevLogTerm,
						LeaderCommit: commitIndex,
						Entries:      log[start:end],
					}
					reply := RequestAppendEntriesReply{}
					if ok := rf.sendRequestAppendEntries(server, &args, &reply); ok {
						if reply.Success {
							rf.mu.Lock()
							rf.matchIndex[server] = end
							rf.mu.Unlock()
						}
						//DPrintf("%d server %d heartbeat with server %d failed\n", rf.me, server, t.UnixNano()/1e6)
					} else {
						//DPrintf("%d server %d heartbeat with server %d failed\n", rf.me, server, t.UnixNano()/1e6)
					}
					DPrintf("server %d leader send to client %d %v reply %v\n", rf.me, server, log[start:end], reply)

				}(i, prevLogIndex, prevLogTerm, commitIndex, start, end, log)
			} else {
				go func(server, currentTerm, commitIndex int) {
					heatBeatArgs := RequestAppendEntriesArgs{
						Term:         currentTerm,
						LeaderId:     rf.me,
						LeaderCommit: commitIndex,
					}
					reply := RequestAppendEntriesReply{}
					if ok := rf.sendRequestAppendEntries(server, &heatBeatArgs, &reply); ok {
						//DPrintf("%d server %d heartbeat with server %d failed\n", rf.me, server, t.UnixNano()/1e6)
					} else {
						//DPrintf("%d server %d heartbeat with server %d failed\n", rf.me, server, t.UnixNano()/1e6)
					}
				}(i, currentTerm, commitIndex)
			}
		}
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
		DPrintf("server %d %s -> %s\n", rf.me, stateArray[prev], stateArray[next])
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
		DPrintf("server %d %s -> %s\n", rf.me, stateArray[prev], stateArray[next])
		if rf.state == LEADER {
			length := len(rf.log)
			rf.nextIndex = nil
			rf.matchIndex = nil
			for i := 0; i < len(rf.peers); i++ {
				rf.nextIndex = append(rf.nextIndex, length)
				rf.matchIndex = append(rf.matchIndex, -1)
			}
		}
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
		DPrintf("server %d %s -> %s in %d term \n", rf.me, stateArray[prev], stateArray[next], newTerm)
		if rf.state == LEADER {
			length := len(rf.log)
			rf.nextIndex = nil
			rf.matchIndex = nil
			for i := 0; i < len(rf.peers); i++ {
				rf.nextIndex = append(rf.nextIndex, length)
				rf.matchIndex = append(rf.matchIndex, 0)
			}
		}
		return true
	} else {
		return false
	}
}

func (rf *Raft) voteSelfAndIncrementTerm() (newTerm int, ok bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	newTerm = rf.currentTerm + 1
	if v, ok := rf.votedFor[newTerm]; !ok && rf.state == CANDIDATE {
		rf.votedFor[newTerm] = rf.me
		rf.currentTerm = newTerm
		return newTerm, true
	} else {
		if rf.state != CANDIDATE {
			DPrintf("server %d in term %d vote self failed (%s)", newTerm, newTerm, stateArray[rf.state])
		} else if !ok {
			DPrintf("server %d in term %d vote self failed (has vote %d)", newTerm, newTerm, v)
		}
		return rf.currentTerm, false
	}
}

func (rf *Raft) updateCommit() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for {
		matchCount := 0
		DPrintf("server %d commitIndex = %d log = %v\n", rf.me, rf.commitIndex, rf.log)
		for i := 0; i < len(rf.peers); i++ {
			if rf.matchIndex[i] > rf.commitIndex {
				matchCount++
			}
		}
		if matchCount >= len(rf.peers)/2+1 {
			rf.commitIndex++
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.commitIndex-1].Command,
				CommandIndex: rf.commitIndex,
			}
		} else {
			break
		}
	}
}

func (rf *Raft) getAllState() (currentTerm, state, commitIndex int, matchIndex []int, nextIndex []int, log []Entry) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state, rf.commitIndex, rf.matchIndex, rf.nextIndex, rf.log
}

func (rf *Raft) initStateMachine() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex = append(rf.nextIndex, rf.lastApplied+1)
		rf.matchIndex = append(rf.nextIndex, 0)
	}

}
