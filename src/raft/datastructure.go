package raft

type Entry struct {
	Term    int
	Index   int
	Command interface{}
}

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term         int
	VoteGranted  bool
	LastLogIndex int
	LastLogTerm  int
}

type RequestAppendEntriesArgs struct {
	Term            int     // leader’s term
	LeaderId        int     // so follower can redirect clients
	PrevLogIndex    int     // index of log entry immediately preceding new ones
	PrevLogTerm     int     // term of prevLogIndex entry
	Entries         []Entry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommitted int     // leader’s committedIndex
}

type RequestAppendEntriesReply struct {
	Term          int  // currentTerm, for leader to update itself
	Success       bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	ConflictIndex int  // conflict index for follower log and leader RequestAppendEntries args
}

type RequestInstallSnapshotArgs struct {
	Term              int // leader’s term
	LeaderId          int // so follower can redirect clients
	LastIncludedIndex int // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int // term of lastIncludedIndex
	//Offset            int    // byte offset where chunk is positioned in the snapshot file
	Data []byte // raw bytes of the snapshot chunk, starting at offset
	//Done bool   // true if this is the last chunk
}

type RequestInstallSnapshotReply struct {
	Term int // currentTerm, for leader to update itself
}
