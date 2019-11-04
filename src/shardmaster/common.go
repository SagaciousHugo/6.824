package shardmaster

import (
	"fmt"
	"raft"
	"sync"
)

//
// Master shard server: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

const (
	OK                 = "OK"
	ErrConfigNum       = "ErrConfigNum"
	ErrWrongLeader     = "ErrWrongLeader"
	ErrOpTimeout       = "ErrOpTimeout"
	ErrKVServerClosed  = "ErrKVServerClosed"
	ErrOutdatedRequest = "ErrOutdatedRequest"
	OpTimeout          = raft.ELECTIONTIMEOUT
	ClientOpWait       = raft.HEARTBEAT
	NotifyKeyFormat    = "%d_%d"
)

type Err string

type OpInfo interface {
	getClientId() int64
	getOpId() int64
}

type BaseArgs struct {
	ClientId int64
	OpId     int64
}

func (b BaseArgs) getClientId() int64 {
	return b.ClientId
}

func (b BaseArgs) getOpId() int64 {
	return b.OpId
}

type JoinArgs struct {
	BaseArgs
	Servers map[int][]string // new GID -> servers mappings
}

type JoinReply struct {
	Result string
}

type LeaveArgs struct {
	BaseArgs
	GIDs []int
}

type LeaveReply struct {
	Result string
}

type MoveArgs struct {
	BaseArgs
	Shard int
	GID   int
}

type MoveReply struct {
	Result string
}

type QueryArgs struct {
	BaseArgs
	Num int // desired config number
}

type QueryReply struct {
	Result string
	Config Config
}

type OpResult struct {
	ClientId int64
	OpId     int64
	Result   string
	Config   Config
}

type Wait struct {
	l sync.RWMutex
	m map[string]chan OpResult
}

// New creates a Wait.
func NewWait() *Wait {
	return &Wait{m: make(map[string]chan OpResult)}
}

func (w *Wait) Register(info OpInfo) <-chan OpResult {
	w.l.Lock()
	defer w.l.Unlock()
	key := fmt.Sprintf("%d_%d", info.getClientId(), info.getOpId())
	ch := w.m[key]
	if ch != nil {
		close(ch)
	}
	ch = make(chan OpResult, 1)
	w.m[key] = ch
	return ch
}

func (w *Wait) Unregister(info OpInfo) {
	w.l.Lock()
	defer w.l.Unlock()
	key := fmt.Sprintf("%d_%d", info.getClientId(), info.getOpId())
	ch := w.m[key]
	delete(w.m, key)
	if ch != nil {
		close(ch)
	}
}

func (w *Wait) Trigger(result OpResult) {
	w.l.RLock()
	defer w.l.RUnlock()
	key := fmt.Sprintf("%d_%d", result.ClientId, result.OpId)
	ch := w.m[key]
	if ch != nil {
		ch <- result
	}
}
