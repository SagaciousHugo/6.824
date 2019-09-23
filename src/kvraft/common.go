package raftkv

import "raft"

const (
	OK                 = "OK"
	ErrNoKey           = "ErrNoKey"
	ErrWrongLeader     = "ErrWrongLeader"
	ErrOpTimeout       = "ErrOpTimeout"
	ErrKVServerClosed  = "ErrKVServerClosed"
	ErrOutdatedRequest = "ErrOutdatedRequest"
	OpTimeout          = raft.ELECTIONTIMEOUT
	ClientOpWait       = raft.HEARTBEAT
	NotifyKeyFormat    = "%d_%d"
)

// Put or Append
type PutAppendArgs struct {
	Key      string
	Value    string
	Op       string // "Put" or "Append"
	OpId     int64
	ClientId int64
}

type PutAppendReply struct {
	Result string
}

// Get
type GetArgs struct {
	Key      string
	OpId     int64
	ClientId int64
}

type GetReply struct {
	Result string
	Value  string
}
