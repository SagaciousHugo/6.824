package raftkv

import (
	"bytes"
	"context"
	"fmt"
	"labgob"
	"labrpc"
	"raft"
	"reflect"
	"sync"
	"time"
)

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

type KVServer struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	ctx          context.Context
	close        context.CancelFunc
	persister    *raft.Persister
	maxraftstate int // snapshot if log grows this big

	database          map[string]string        // store kv data
	lastOpResultStore map[int64]OpResult       // store client last op result
	opResultNotifyChs map[string]chan OpResult // for notify the op has finished
	// Your definitions here.
}

func (kv *KVServer) start(args interface{}) (result string, value string) {
	var op OpArgs
	if getArgs, ok := args.(GetArgs); ok {
		op = OpArgs{
			ClientId: getArgs.ClientId,
			OpId:     getArgs.OpId,
			Key:      getArgs.Key,
			Value:    "",
			OpType:   "Get",
		}
	} else if putAppendArgs, ok := args.(PutAppendArgs); ok {
		op = OpArgs{
			ClientId: putAppendArgs.ClientId,
			OpId:     putAppendArgs.OpId,
			Key:      putAppendArgs.Key,
			Value:    putAppendArgs.Value,
			OpType:   putAppendArgs.Op,
		}
	} else {
		return fmt.Sprintf("ErrArgsType:%+v", args), ""
	}
	kv.mu.Lock()
	if lastOpResult, ok := kv.lastOpResultStore[op.ClientId]; ok && lastOpResult.OpId == op.OpId {
		kv.mu.Unlock()
		return lastOpResult.Result, lastOpResult.Value
	} else if lastOpResult.OpId > op.OpId {
		kv.mu.Unlock()
		return ErrOutdatedRequest, ""
	}
	resultCh := make(chan OpResult, 1)
	kv.opResultNotifyChs[fmt.Sprintf(NotifyKeyFormat, op.ClientId, op.OpId)] = resultCh
	kv.mu.Unlock()
	defer func() {
		kv.mu.Lock()
		delete(kv.opResultNotifyChs, fmt.Sprintf(NotifyKeyFormat, op.ClientId, op.OpId))
		kv.mu.Unlock()
	}()
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return ErrWrongLeader, ""
	}
	//DPrintf("KVServer %d start command %+v index=%d\n", kv.me, op, commandIndex)
	t := time.NewTimer(OpTimeout)
	select {
	case <-kv.ctx.Done():
		return ErrKVServerClosed, ""
	case <-t.C:
		return ErrOpTimeout, ""
	case opResult := <-resultCh:
		return opResult.Result, opResult.Value
		/*if opResult.ClientId != op.ClientId || opResult.OpId != op.OpId {
			return ErrWrongLeader, ""
		} else {


		}*/
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	reply.Result, reply.Value = kv.start(*args)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	reply.Result, _ = kv.start(*args)
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	kv.close()
	// Your code here, if desired.
}

func (kv *KVServer) stateMachine() {
	for {
		select {
		case <-kv.ctx.Done():
			DPrintf("KVServer %d stateMachine closed\n", kv.me)
			return
		case applyMsg := <-kv.applyCh:
			if applyMsg.CommandValid {
				result := OpResult{}
				if op, ok := applyMsg.Command.(OpArgs); !ok {
					DPrintf("KVServer %d stateMachine received wrong type command %+v %v\n", kv.me, applyMsg, reflect.TypeOf(applyMsg.Command))
				} else {
					DPrintf("KVServer %d stateMachine received command %+v\n", kv.me, applyMsg)
					if op.OpType == "Get" {
						kv.mu.Lock()
						if lastOpResult, ok := kv.lastOpResultStore[op.ClientId]; !ok || op.OpId > lastOpResult.OpId {
							if value, ok := kv.database[op.Key]; ok {
								result.Result = OK
								result.Value = value
								result.OpId = op.OpId
								result.ClientId = op.ClientId
							} else {
								result.Result = ErrNoKey
								result.Value = ""
								result.OpId = op.OpId
								result.ClientId = op.ClientId
							}
							kv.lastOpResultStore[op.ClientId] = result
							if resultCh, ok := kv.opResultNotifyChs[fmt.Sprintf(NotifyKeyFormat, op.ClientId, op.OpId)]; ok {
								resultCh <- result
							}
						}
						kv.mu.Unlock()
					} else if op.OpType == "Put" {
						kv.mu.Lock()
						if lastOpResult, ok := kv.lastOpResultStore[op.ClientId]; !ok || op.OpId > lastOpResult.OpId {
							result.Result = OK
							result.Value = ""
							result.OpId = op.OpId
							result.ClientId = op.ClientId
							kv.database[op.Key] = op.Value
							kv.lastOpResultStore[op.ClientId] = result
							if resultCh, ok := kv.opResultNotifyChs[fmt.Sprintf(NotifyKeyFormat, op.ClientId, op.OpId)]; ok {
								resultCh <- result
							}
						}
						kv.mu.Unlock()

					} else if op.OpType == "Append" {
						kv.mu.Lock()
						if lastOpResult, ok := kv.lastOpResultStore[op.ClientId]; !ok || op.OpId > lastOpResult.OpId {
							result.Result = OK
							result.Value = ""
							result.OpId = op.OpId
							result.ClientId = op.ClientId
							oldValue, _ := kv.database[op.Key]
							kv.database[op.Key] = oldValue + op.Value
							kv.lastOpResultStore[op.ClientId] = result
							if resultCh, ok := kv.opResultNotifyChs[fmt.Sprintf(NotifyKeyFormat, op.ClientId, op.OpId)]; ok {
								resultCh <- result
							}
						}
						kv.mu.Unlock()
					}
					kv.saveKVServerState(applyMsg.CommandIndex)
				}
			} else if command, ok := applyMsg.Command.(string); ok {
				if command == raft.CommandInstallSnapshot {
					DPrintf("KVServer %d stateMachine received InstallSnapshot %+v\n", kv.me, applyMsg)
					kv.init()
					kv.rf.InstallSnapshotFinishedAndReplay()
				}
			}
		}
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(OpArgs{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	ctx, cancel := context.WithCancel(context.Background())
	kv.ctx = ctx
	kv.close = cancel
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.persister = persister
	kv.opResultNotifyChs = make(map[string]chan OpResult)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	// You may need initialization code here.
	go kv.stateMachine()
	kv.init()
	kv.rf.Replay()
	return kv
}

func (kv *KVServer) init() {
	data := kv.persister.ReadSnapshot()
	if len(data) == 0 {
		kv.database = make(map[string]string)
		kv.lastOpResultStore = make(map[int64]OpResult)
	} else {
		r := bytes.NewBuffer(data)
		d := labgob.NewDecoder(r)
		kv.database = nil
		kv.lastOpResultStore = nil
		d.Decode(&kv.database)
		d.Decode(&kv.lastOpResultStore)
		//DPrintf("KVServer %d init database = %v globalsessions = %v\n", kv.me, kv.database, kv.globalsessions)
	}
}

func (kv *KVServer) saveKVServerState(lastIncludedIndex int) {
	if kv.maxraftstate == -1 || kv.persister.RaftStateSize() < kv.maxraftstate {
		return
	} else {
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(kv.database)
		e.Encode(kv.lastOpResultStore)
		snapshot := w.Bytes()
		kv.rf.SaveSnapshot(lastIncludedIndex, snapshot)
	}
}
