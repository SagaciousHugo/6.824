package raftkv

import (
	"fmt"
	"raft"
	"sync"
)

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

type OpArgs struct {
	ClientId int64
	OpId     int64
	Key      string
	Value    string
	OpType   string // "Get", "Put" or "Append"
}

type OpResult struct {
	ClientId int64
	OpId     int64
	Result   string
	Value    string
}

// 参考：https://github.com/etcd-io/etcd/tree/master/pkg/wait
type Wait struct {
	l sync.RWMutex
	m map[string]chan OpResult
}

// New creates a Wait.
func NewWait() *Wait {
	return &Wait{m: make(map[string]chan OpResult)}
}

func (w *Wait) Register(op OpArgs) <-chan OpResult {
	w.l.Lock()
	defer w.l.Unlock()
	key := fmt.Sprintf(NotifyKeyFormat, op.ClientId, op.OpId)
	ch := w.m[key]
	if ch != nil {
		close(ch)
	}
	ch = make(chan OpResult, 1)
	w.m[key] = ch
	return ch
}

func (w *Wait) Unregister(op OpArgs) {
	w.l.Lock()
	defer w.l.Unlock()
	key := fmt.Sprintf(NotifyKeyFormat, op.ClientId, op.OpId)
	ch := w.m[key]
	delete(w.m, key)
	if ch != nil {
		close(ch)
	}
}

func (w *Wait) Trigger(result OpResult) {
	w.l.RLock()
	defer w.l.RUnlock()
	key := fmt.Sprintf(NotifyKeyFormat, result.ClientId, result.OpId)
	ch := w.m[key]
	if ch != nil {
		ch <- result
	}
}
