package shardkv

import (
	"fmt"
	"log"
	"shardmaster"
	"sync"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

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
	key := fmt.Sprintf("%d_%d", op.ClientId, op.OpId)
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
	key := fmt.Sprintf("%d_%d", op.ClientId, op.OpId)
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

func NewWaitMigration() *WaitMigration {
	wm := WaitMigration{}
	wm.Init(shardmaster.Config{
		Groups: make(map[int][]string),
	})
	return &wm
}

type WaitMigration struct {
	Config    shardmaster.Config
	Shards    map[int]struct{}
	GidShards map[int][]int
}

func (wm *WaitMigration) Init(config shardmaster.Config) {
	wm.Config = config
	wm.Shards = make(map[int]struct{})
	wm.GidShards = make(map[int][]int)
}

func (wm *WaitMigration) GetConfig() shardmaster.Config {
	return wm.Config
}

func (wm *WaitMigration) IsEmpty() bool {
	return len(wm.GidShards) == 0 && len(wm.Shards) == 0
}

func (wm *WaitMigration) GetShardByGid(gid int) (shards []int) {
	shards = wm.GidShards[gid]
	return
}

func (wm *WaitMigration) IsMigrationShard(shardNum int) (ok bool) {
	_, ok = wm.Shards[shardNum]
	return
}

func (wm *WaitMigration) DeleteByGid(gid int) (ok bool) {
	shards, ok := wm.GidShards[gid]
	if ok {
		delete(wm.GidShards, gid)
		for i := range shards {
			delete(wm.Shards, shards[i])
		}
	}
	return

}

func (wm *WaitMigration) AddGidShard(gid, shardNum int) {
	wm.Shards[shardNum] = Exists
	shards, _ := wm.GidShards[gid]
	shards = append(shards, shardNum)
	wm.GidShards[gid] = shards
}

func (wm *WaitMigration) GetGidShards() map[int][]int {
	return wm.GidShards
}

func (wm *WaitMigration) Clear() {
	wm.Shards = make(map[int]struct{})
	wm.GidShards = make(map[int][]int)
}

func NewWaitClean() *WaitClean {
	wc := WaitClean{}
	wc.Init(shardmaster.Config{
		Groups: make(map[int][]string),
	})
	return &wc
}

type WaitClean struct {
	Config    shardmaster.Config
	Shards    map[int]struct{}
	GidShards map[int][]int
	Database  map[int]Shard
}

func (wc *WaitClean) GetConfig() shardmaster.Config {
	return wc.Config
}

func (wc *WaitClean) Init(config shardmaster.Config) {
	wc.Config = config
	wc.Shards = make(map[int]struct{})
	wc.GidShards = make(map[int][]int)
	wc.Database = make(map[int]Shard)
}

func (wc *WaitClean) IsEmpty() bool {
	return len(wc.GidShards) == 0 && len(wc.Shards) == 0
}

func (wc *WaitClean) AddGidShard(gid, shardNum int) {
	wc.Shards[shardNum] = Exists
	shards, _ := wc.GidShards[gid]
	shards = append(shards, shardNum)
	wc.GidShards[gid] = shards
}

func (wc *WaitClean) IsCleanShard(shardNum int) (ok bool) {
	_, ok = wc.Shards[shardNum]
	return
}
func (wc *WaitClean) GetGidShard() map[int][]int {
	return wc.GidShards
}

func (wc *WaitClean) DeleteByGid(gid int) (ok bool) {
	shards, ok := wc.GidShards[gid]
	if ok {
		delete(wc.GidShards, gid)
		for i := range shards {
			delete(wc.Shards, shards[i])
			delete(wc.Database, shards[i])
		}
	}
	return
}

func (wc *WaitClean) StoreCleanData(database *ShardDatabase) {
	for shardNum := range wc.Shards {
		wc.Database[shardNum] = database.GetShard(shardNum)
		database.DeleteShard(shardNum)
	}
}

func (wc *WaitClean) GetShard(shardNum int) (shard Shard, ok bool) {
	shard, ok = wc.Database[shardNum]
	return
}

func (wc *WaitClean) Clear() {
	wc.Shards = make(map[int]struct{})
	wc.GidShards = make(map[int][]int)
	wc.Database = make(map[int]Shard)
}

func NewShardDatabase() *ShardDatabase {
	database := ShardDatabase{}
	database.ShardData = make(map[int]Shard)
	return &database
}

type ShardDatabase struct {
	ShardData map[int]Shard
}

func (sd *ShardDatabase) GetShard(shardNum int) Shard {
	shard, ok := sd.ShardData[shardNum]
	if !ok {
		shard = NewShard()
		sd.ShardData[shardNum] = shard
	}
	return shard
}

func (sd *ShardDatabase) SetShard(shardNum int, shard Shard) {
	sd.ShardData[shardNum] = shard
}

func (sd *ShardDatabase) DeleteShard(shardNum int) {
	delete(sd.ShardData, shardNum)
}

func NewShard() Shard {
	return Shard{
		Data:     make(map[string]string),
		LastOpId: make(map[int64]int64),
	}
}

type Shard struct {
	Data     map[string]string
	LastOpId map[int64]int64
}

func (s *Shard) Get(key string) (value string, ok bool) {
	value, ok = s.Data[key]
	return
}

func (s *Shard) Put(key, value string, clientId, opId int64) {
	if lastOpId, ok := s.LastOpId[clientId]; !ok || opId > lastOpId {
		s.Data[key] = value
		s.LastOpId[clientId] = opId
	}
}

func (s *Shard) Append(key, value string, clientId, opId int64) {
	if lastOpId, ok := s.LastOpId[clientId]; !ok || opId > lastOpId {
		old := s.Data[key]
		s.Data[key] = old + value
		s.LastOpId[clientId] = opId
	}
}
