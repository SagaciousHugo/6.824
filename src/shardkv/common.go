package shardkv

import (
	"raft"
	"time"
)

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-Time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from Time to Time.
//
// You will have to modify these definitions.
//

const (
	OK                      = "OK"
	ErrNoKey                = "ErrNoKey"
	ErrWrongGroup           = "ErrWrongGroup"
	ErrWrongLeader          = "ErrWrongLeader"
	ErrNewConfIsApplying    = "ErrNewConfIsApplying"
	ErrNotReceiveNewConf    = "ErrNotReceiveNewConf"
	ErrShardHasBeenCleaned  = "ErrShardHasBeenCleaned"
	ErrShardIsMigrating     = "ErrShardIsMigrating"
	ErrShardKVClosed        = "ErrShardKVClosed"
	ErrOpTimeout            = "ErrOpTimeout"
	ErrWaitMigrationTimeOut = "ErrWaitMigrationTimeOut"
	ErrWaitCleanTimeOut     = "ErrWaitCleanTimeOut"
	OpTimeout               = raft.ELECTIONTIMEOUT
	WaitMigrationTimeOut    = 2 * time.Second
	WaitCleanTimeOut        = 3 * time.Second
)

var Exists = struct{}{}

type OpArgs struct {
	ConfigNum int
	ClientId  int64
	OpId      int64
	Key       string
	Value     string
	OpType    string // "Get", "Put" or "Append"
}

type OpResult struct {
	ClientId int64
	OpId     int64
	Result   string
	Value    string
}

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	ConfigNum int
	Key       string
	Value     string
	Op        string // "Put" or "Append"
	ClientId  int64
	OpId      int64
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Result string
}

type GetArgs struct {
	ConfigNum int
	Key       string
	ClientId  int64
	OpId      int64
	// You'll have to add definitions here.
}

type GetReply struct {
	Result string
	Value  string
}

type MigrateShardArgs struct {
	ConfigNum int
	Gid       int
	ShardNums []int
}

type MigrateShardReply struct {
	ConfigNum int
	Gid       int
	Result    string
	Data      map[int]Shard
}

type CheckMigrateShardArgs struct {
	ConfigNum int
	Gid       int
	ShardNums []int
}

type CheckMigrateShardReply struct {
	ConfigNum int
	Gid       int
	Result    string
}
