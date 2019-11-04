package shardmaster

import (
	"bytes"
	"container/heap"
	"context"
	"encoding/gob"
	"fmt"
	"log"
	"raft"
	"reflect"
	"time"
)
import "labrpc"
import "sync"
import "labgob"

func init() {
	labgob.Register(OpResult{})
	labgob.Register(JoinArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(QueryArgs{})
}

type ShardMaster struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	ctx     context.Context
	close   context.CancelFunc
	// Your data here.
	persister         *raft.Persister
	lastApplied       int
	currentConfig     Config
	lastOpResultStore map[int64]OpResult // store client last op result
	applyWait         *Wait
	configs           []Config // indexed by config num
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	reply.Result, _ = sm.start(*args)
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	reply.Result, _ = sm.start(*args)
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	reply.Result, _ = sm.start(*args)
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sm.mu.RLock()
	if lastOpResult, ok := sm.lastOpResultStore[args.getClientId()]; ok && lastOpResult.OpId == args.getOpId() {
		reply.Result = lastOpResult.Result
		reply.Config = lastOpResult.Config
		sm.mu.RUnlock()
		return
	} else if lastOpResult.OpId > args.getOpId() {
		reply.Result = ErrOutdatedRequest
		sm.mu.RUnlock()
		return
	} else if args.Num >= 0 && args.Num < len(sm.configs) {
		reply.Result = OK
		reply.Config = sm.configs[args.Num]
		sm.lastOpResultStore[args.ClientId] = OpResult{args.ClientId, args.OpId, OK, reply.Config}
		sm.mu.RUnlock()
		return
	}
	sm.mu.RUnlock()

	resultCh := sm.applyWait.Register(args)
	defer sm.applyWait.Unregister(args)

	_, _, isLeader := sm.rf.Start(*args)
	if !isLeader {
		reply.Result = ErrWrongLeader
		return
	}
	t := time.NewTimer(OpTimeout)
	select {
	case <-sm.ctx.Done():
		reply.Result = ErrKVServerClosed
	case <-t.C:
		reply.Result = ErrOpTimeout
		DPrintf("ShardMaster %d return client %d by resultCh args = %+v result = %+v\n", sm.me, args.ClientId, args, reply.Result)

	case opResult := <-resultCh:
		DPrintf("ShardMaster %d return client %d by resultCh args = %+v result = %+v\n", sm.me, args.ClientId, args, opResult)
		reply.Result = opResult.Result
		reply.Config = opResult.Config
	}
}

func (sm *ShardMaster) start(args interface{}) (result string, config Config) {
	info, ok := args.(OpInfo)
	if !ok {
		return fmt.Sprintf("ErrArgsType:%+v", args), config
	}
	sm.mu.RLock()
	if lastOpResult, ok := sm.lastOpResultStore[info.getClientId()]; ok && lastOpResult.OpId == info.getOpId() {
		sm.mu.RUnlock()
		return lastOpResult.Result, lastOpResult.Config
	} else if lastOpResult.OpId > info.getOpId() {
		sm.mu.RUnlock()
		return ErrOutdatedRequest, config
	}
	sm.mu.RUnlock()
	resultCh := sm.applyWait.Register(info)
	defer sm.applyWait.Unregister(info)

	_, _, isLeader := sm.rf.Start(args)
	if !isLeader {
		return ErrWrongLeader, config
	}

	t := time.NewTimer(OpTimeout)
	select {
	case <-sm.ctx.Done():
		return ErrKVServerClosed, config
	case <-t.C:
		return ErrOpTimeout, config
	case opResult := <-resultCh:
		DPrintf("ShardMaster %d return client %d by resultCh result = %+v\n", sm.me, info.getClientId(), opResult)
		return opResult.Result, opResult.Config
	}
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
	sm.close()
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me
	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	ctx, cancel := context.WithCancel(context.Background())
	sm.ctx = ctx
	sm.close = cancel
	sm.persister = persister
	sm.applyWait = NewWait()
	sm.init()
	go sm.stateMachine()
	return sm
}

func (sm *ShardMaster) init() {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	data := sm.persister.ReadSnapshot()
	if len(data) == 0 {
		sm.configs = make([]Config, 1)
		sm.lastApplied = 0
		sm.configs[0].Groups = map[int][]string{}
		sm.lastOpResultStore = make(map[int64]OpResult)
	} else {
		r := bytes.NewBuffer(data)
		d := labgob.NewDecoder(r)
		sm.configs = nil
		sm.lastOpResultStore = nil
		sm.lastApplied = 0
		d.Decode(&sm.configs)
		d.Decode(&sm.lastOpResultStore)
		d.Decode(&sm.lastApplied)
	}
}

func (sm *ShardMaster) saveShardMasterState() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(sm.configs)
	e.Encode(sm.lastOpResultStore)
	e.Encode(sm.lastApplied)
	snapshot := w.Bytes()
	sm.rf.SaveSnapshot(sm.lastApplied, snapshot)
}

func (sm *ShardMaster) stateMachine() {
	for {
		select {
		case <-sm.ctx.Done():
			DPrintf("ShardMaster %d stateMachine closed\n", sm.me)
			return
		case applyMsg := <-sm.applyCh:
			if applyMsg.CommandValid {
				result := OpResult{}
				sm.mu.Lock()
				if sm.lastApplied+1 < applyMsg.CommandIndex {
					go sm.rf.Replay()
				} else if applyMsg.CommandIndex == sm.lastApplied+1 {
					sm.lastApplied++
					switch op := applyMsg.Command.(type) {
					case JoinArgs:
						if lastOpResult, ok := sm.lastOpResultStore[op.ClientId]; !ok || op.OpId > lastOpResult.OpId {
							sm.joinGroups(op)
							result.ClientId = op.ClientId
							result.OpId = op.OpId
							result.Result = OK
							sm.lastOpResultStore[op.ClientId] = result
							sm.saveShardMasterState()
							go sm.applyWait.Trigger(result)
						}
					case MoveArgs:
						if lastOpResult, ok := sm.lastOpResultStore[op.ClientId]; !ok || op.OpId > lastOpResult.OpId {
							sm.moveShardGroup(op)
							result.ClientId = op.ClientId
							result.OpId = op.OpId
							result.Result = OK
							sm.lastOpResultStore[op.ClientId] = result
							sm.saveShardMasterState()
							go sm.applyWait.Trigger(result)
						}
					case LeaveArgs:
						if lastOpResult, ok := sm.lastOpResultStore[op.ClientId]; !ok || op.OpId > lastOpResult.OpId {
							sm.leaveGroups(op)
							result.ClientId = op.ClientId
							result.OpId = op.OpId
							result.Result = OK
							sm.lastOpResultStore[op.ClientId] = result
							sm.saveShardMasterState()
							go sm.applyWait.Trigger(result)
						}
					case QueryArgs:
						if lastOpResult, ok := sm.lastOpResultStore[op.ClientId]; !ok || op.OpId > lastOpResult.OpId {
							result.ClientId = op.ClientId
							result.OpId = op.OpId
							if op.Num >= 0 && op.Num < len(sm.configs) {
								result.Result = OK
								result.Config = sm.configs[op.Num]
							} else if op.Num < 0 {
								result.Result = OK
								result.Config = sm.configs[len(sm.configs)-1]
							} else {
								result.Result = ErrConfigNum
							}
							sm.lastOpResultStore[op.ClientId] = result
							sm.saveShardMasterState()
							go sm.applyWait.Trigger(result)
						}
					default:
						DPrintf("ShardMaster %d stateMachine received wrong type command %+v %v\n", sm.me, applyMsg, reflect.TypeOf(applyMsg.Command))
					}
				}
				sm.mu.Unlock()

			} else if command, ok := applyMsg.Command.(string); ok {
				if command == raft.CommandInstallSnapshot {
					DPrintf("ShardMaster %d stateMachine received InstallSnapshot %+v\n", sm.me, applyMsg)
					sm.init()
					sm.rf.Replay()
				}
			}
		}
	}
}

func (sm *ShardMaster) joinGroups(args JoinArgs) {
	conf := sm.generateNewConfig()
	for gid, servers := range args.Servers {
		conf.Groups[gid] = servers
	}
	sm.reBalance(conf)
	sm.configs = append(sm.configs, *conf)
	DPrintf("ShardMaster %d join configs = %v\n", sm.me, sm.configs)

}

func (sm *ShardMaster) leaveGroups(args LeaveArgs) {
	conf := sm.generateNewConfig()
	for _, gid := range args.GIDs {
		delete(conf.Groups, gid)
	}
	sm.reBalance(conf)
	sm.configs = append(sm.configs, *conf)
	DPrintf("ShardMaster %d leave configs = %v\n", sm.me, sm.configs)

}

func (sm *ShardMaster) moveShardGroup(args MoveArgs) {
	conf := sm.generateNewConfig()
	conf.Shards[args.Shard] = args.GID
	DPrintf("ShardMaster %d move configs = %v\n", sm.me, sm.configs)
}

func (sm *ShardMaster) reBalance(conf *Config) {
	if len(conf.Groups) == 0 {
		conf.Shards = [NShards]int{}
		return
	}
	divide := shardDivide(len(conf.Shards), len(conf.Groups))
	var reallocateShards []int

	h := NewHeapGroupShard(conf.Groups)

	for i, gid := range conf.Shards {
		if _, ok := conf.Groups[gid]; !ok {
			reallocateShards = append(reallocateShards, i)
		} else {
			h.addGs(gid, i)
		}
	}
	for h.Len() > 0 {
		gs := heap.Pop(h).(*groupShard)
		if len(gs.Shards) > divide[0] {
			index := divide[0]
			reallocateShards = append(reallocateShards, gs.Shards[index:]...)
			gs.Shards = gs.Shards[:index]
		} else if len(gs.Shards) < divide[0] {
			index := divide[0] - len(gs.Shards)
			gs.Shards = append(gs.Shards, reallocateShards[:index]...)
			reallocateShards = reallocateShards[index:]
		}
		for k := 0; k < len(gs.Shards); k++ {
			conf.Shards[gs.Shards[k]] = gs.Gid
		}
		divide = divide[1:]
	}
	return
}

func (sm *ShardMaster) generateNewConfig() (conf *Config) {
	var buf bytes.Buffer
	conf = &Config{}
	latest := sm.configs[len(sm.configs)-1]
	if err := gob.NewEncoder(&buf).Encode(latest); err != nil {
		log.Println(err)
		return
	}
	if err := gob.NewDecoder(bytes.NewBuffer(buf.Bytes())).Decode(conf); err != nil {
		log.Println(err)
	}
	conf.Num++
	return
}
