package raft

type Entry struct {
	Term    int
	Index   int
	Command interface{}
}

// import "bytes"
// import "labgob"

//
// as each Raft peer becomes aware that successive log entries are
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

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term                   int
	CandidateId            int
	LastCommittingLogIndex int
	LastCommittingLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term                   int
	VoteGranted            bool
	LastCommittingLogIndex int
	LastCommittingLogTerm  int
}

type RequestAppendEntriesArgs struct {
	Term             int     // leader’s term
	LeaderId         int     // so follower can redirect clients
	PrevLogIndex     int     // index of log entry immediately preceding new ones
	PrevLogTerm      int     // term of prevLogIndex entry
	Entries          []Entry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommitting int     // leader’s committingIndex
	LeaderCommitted  int     // leader’s committedIndex
}

type RequestAppendEntriesReply struct {
	Term               int  // currentTerm, for leader to update itself
	Success            bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	ConflictIndex      int
	FollowerCommitting int // follower's committingIndex
}
