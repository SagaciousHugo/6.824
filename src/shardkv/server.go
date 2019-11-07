package shardkv

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"labgob"
	"labrpc"
	"raft"
	"reflect"
	"shardmaster"
	"sync"
	"time"
)

func init() {
	labgob.Register(OpArgs{})
	labgob.Register(GetArgs{})
	labgob.Register(PutAppendArgs{})
	labgob.Register(shardmaster.Config{})
	labgob.Register(CheckMigrateShardReply{})
	labgob.Register(MigrateShardReply{})
}

type ShardKV struct {
	mu            sync.RWMutex
	me            int
	rf            *raft.Raft
	applyCh       chan raft.ApplyMsg
	make_end      func(string) *labrpc.ClientEnd
	gid           int
	masters       *shardmaster.Clerk
	maxraftstate  int // snapshot if log grows this big
	persister     *raft.Persister
	ctx           context.Context
	close         context.CancelFunc
	lastApplied   int
	configs       []shardmaster.Config
	database      *ShardDatabase
	applyWait     *Wait
	lastOpId      map[int64]int64 // store client last op id
	waitMigration *WaitMigration  // store migration shard
	waitClean     *WaitClean      // store clean shard
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	kv.close()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = shardmaster.MakeClerk(masters)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.applyWait = NewWait()
	kv.persister = persister
	ctx, cancel := context.WithCancel(context.Background())
	kv.ctx = ctx
	kv.close = cancel
	kv.init()
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	if !kv.waitMigration.IsEmpty() {
		go kv.migrationHelper()
	}
	if !kv.waitClean.IsEmpty() {
		go kv.migrationChecker()
	}
	go kv.newConfigLearner()
	go kv.stateMachine()
	return kv
}

func (kv *ShardKV) start(args interface{}) (result string, value string) {
	var op OpArgs
	if getArgs, ok := args.(GetArgs); ok {
		op = OpArgs{
			ConfigNum: getArgs.ConfigNum,
			ClientId:  getArgs.ClientId,
			OpId:      getArgs.OpId,
			Key:       getArgs.Key,
			Value:     "",
			OpType:    "Get",
		}
	} else if putAppendArgs, ok := args.(PutAppendArgs); ok {
		op = OpArgs{
			ConfigNum: putAppendArgs.ConfigNum,
			ClientId:  putAppendArgs.ClientId,
			OpId:      putAppendArgs.OpId,
			Key:       putAppendArgs.Key,
			Value:     putAppendArgs.Value,
			OpType:    putAppendArgs.Op,
		}
	} else {
		return fmt.Sprintf("ErrArgsType:%+v", args), ""
	}

	resultCh := kv.applyWait.Register(op)
	defer kv.applyWait.Unregister(op)
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return ErrWrongLeader, ""
	}
	t := time.NewTimer(OpTimeout)
	select {
	case <-kv.ctx.Done():
		return ErrShardKVClosed, ""
	case <-t.C:
		return ErrOpTimeout, ""
	case opResult := <-resultCh:
		//DPrintf("ShardKV %d return client %d by resultCh result = %+v\n", kv.me, op.ClientId, opResult)
		return opResult.Result, opResult.Value
	}
}

func (kv *ShardKV) newConfigLearner() {
	t := time.NewTicker(100 * time.Millisecond)
	for {
		select {
		case <-kv.ctx.Done():
			return
		case <-t.C:
			kv.mu.RLock()
			if !kv.waitClean.IsEmpty() || !kv.waitMigration.IsEmpty() {
				// is applying new Config
				kv.mu.RUnlock()
				continue
			}
			latest := kv.configs[len(kv.configs)-1]
			kv.mu.RUnlock()
			config := kv.masters.Query(latest.Num + 1)
			if latest.Num < config.Num {
				kv.rf.Start(config)
			}
		}
	}
}

func (kv *ShardKV) migrationChecker() {
	t := time.NewTicker(100 * time.Millisecond)
	start := time.Now()

	for {
		select {
		case <-kv.ctx.Done():
			return
		case <-t.C:
			kv.mu.RLock()
			if kv.waitClean.IsEmpty() {
				kv.mu.RUnlock()
				return
			}
			gidShards := kv.waitClean.GetGidShard()
			config := kv.waitClean.GetConfig()
			if time.Now().Sub(start) > WaitCleanTimeOut {
				kv.rf.Start(CheckMigrateShardReply{
					ConfigNum: config.Num,
					Gid:       -1,
					Result:    ErrWaitCleanTimeOut,
				})
			} else {
				for gid, shardNums := range gidShards {
					servers := config.Groups[gid]
					args := CheckMigrateShardArgs{
						config.Num,
						gid,
						shardNums,
					}
					go func(servers []string, args CheckMigrateShardArgs) {
						for si := 0; si < len(servers); si++ {
							srv := kv.make_end(servers[si])
							var reply CheckMigrateShardReply
							ok := srv.Call("ShardKV.CheckMigrateShard", &args, &reply)
							/*if ok {
								DPrintf("ShardKV %d (gid = %d) CheckMigrateShard ok = %v args = %+v reply = %+v\n", kv.me, kv.gid, ok, args, reply)
							}*/
							if ok && reply.Result == OK {
								kv.rf.Start(reply)
								return
							}
						}
					}(servers, args)
				}
			}
			kv.mu.RUnlock()
		}
	}
}

func (kv *ShardKV) migrationHelper() {
	t := time.NewTicker(100 * time.Millisecond)
	start := time.Now()
	for {
		select {
		case <-kv.ctx.Done():
			return
		case <-t.C:
			kv.mu.RLock()
			if kv.waitMigration.IsEmpty() {
				kv.mu.RUnlock()
				return
			}
			gidShards := kv.waitMigration.GetGidShards()
			config := kv.waitMigration.GetConfig()
			if time.Now().Sub(start) > WaitMigrationTimeOut {
				kv.rf.Start(MigrateShardReply{
					ConfigNum: config.Num,
					Gid:       -1,
					Result:    ErrWaitMigrationTimeOut,
				})
			} else {
				for gid, shardNums := range gidShards {
					servers := config.Groups[gid]
					args := MigrateShardArgs{
						config.Num,
						gid,
						shardNums,
					}
					go func(servers []string, args MigrateShardArgs) {
						for si := 0; si < len(servers); si++ {
							srv := kv.make_end(servers[si])
							var reply MigrateShardReply
							ok := srv.Call("ShardKV.MigrateShard", &args, &reply)
							DPrintf("ShardKV %d (gid = %d) MigrateShard ok = %v args = %+v reply = %+v\n", kv.me, kv.gid, ok, args, reply)
							if ok && (reply.Result == OK || reply.Result == ErrShardHasBeenCleaned) {
								kv.rf.Start(reply)
								return
							}
						}
					}(servers, args)
				}
			}
			kv.mu.RUnlock()
		}
	}
}

func (kv *ShardKV) init() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	data := kv.persister.ReadSnapshot()
	if len(data) == 0 {
		kv.lastApplied = 0
		kv.database = NewShardDatabase()
		kv.lastOpId = make(map[int64]int64)
		kv.configs = make([]shardmaster.Config, 1)
		kv.configs[0].Groups = map[int][]string{}
		kv.waitClean = NewWaitClean()
		kv.waitMigration = NewWaitMigration()
	} else {
		r := bytes.NewBuffer(data)
		d := labgob.NewDecoder(r)
		kv.lastApplied = 0
		kv.database = nil
		kv.lastOpId = nil
		kv.configs = nil
		kv.waitMigration = nil
		kv.waitClean = nil
		d.Decode(&kv.lastApplied)
		d.Decode(&kv.database)
		d.Decode(&kv.lastOpId)
		d.Decode(&kv.configs)
		d.Decode(&kv.waitClean)
		d.Decode(&kv.waitMigration)
	}
}

func (kv *ShardKV) saveShardKVState(force bool) {
	shouldSave := kv.maxraftstate != -1 && (force || kv.persister.RaftStateSize() > kv.maxraftstate)
	if shouldSave {
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(kv.lastApplied)
		e.Encode(kv.database)
		e.Encode(kv.lastOpId)
		e.Encode(kv.configs)
		e.Encode(kv.waitClean)
		e.Encode(kv.waitMigration)
		snapshot := w.Bytes()
		kv.rf.SaveSnapshot(kv.lastApplied, snapshot)
	}
}

func (kv *ShardKV) stateMachine() {
	for {
		select {
		case <-kv.ctx.Done():
			DPrintf("ShardKV %d (gid=%d)stateMachine closed\n", kv.me, kv.gid)
			return
		case applyMsg := <-kv.applyCh:
			if applyMsg.CommandValid {
				kv.mu.Lock()
				if kv.lastApplied+1 < applyMsg.CommandIndex {
					kv.mu.Unlock()
					kv.rf.Replay()
				} else {
					if kv.lastApplied+1 == applyMsg.CommandIndex {
						kv.lastApplied++
						//DPrintf("ShardKV(gid=%d) %d stateMachine received command %v %+v\n", kv.me, kv.gid, reflect.TypeOf(applyMsg.Command), applyMsg)
						switch command := applyMsg.Command.(type) {
						case OpArgs:
							op := command
							result := OpResult{ClientId: op.ClientId, OpId: op.OpId}
							switch op.OpType {
							case "Get":
								shardNum := key2shard(op.Key)
								latest := kv.configs[len(kv.configs)-1]
								switch {
								case op.ConfigNum != latest.Num || latest.Shards[shardNum] != kv.gid:
									result.Result = ErrWrongGroup
								case kv.waitMigration.IsMigrationShard(shardNum):
									result.Result = ErrShardIsMigrating
								default:
									shard := kv.database.GetShard(shardNum)
									if value, ok := shard.Get(op.Key); ok {
										result.Result = OK
										result.Value = value
									} else {
										result.Result = ErrNoKey
									}
									str, _ := json.Marshal(kv.database)
									DPrintf("ShardKV(gid=%d) %d shardNum %d database= %s get key:%v result: %s\n", kv.gid, kv.me, shardNum, str, op.Key, result.Result)
									kv.saveShardKVState(false)
								}
								go kv.applyWait.Trigger(result)
							case "Put":
								shardNum := key2shard(op.Key)
								latest := kv.configs[len(kv.configs)-1]
								switch {
								case op.ConfigNum != latest.Num || latest.Shards[shardNum] != kv.gid:
									result.Result = ErrWrongGroup
								case kv.waitMigration.IsMigrationShard(shardNum):
									result.Result = ErrShardIsMigrating
								default:
									result.Result = OK
									if lastOpId, ok := kv.lastOpId[op.ClientId]; !ok || op.OpId > lastOpId {
										shard := kv.database.GetShard(shardNum)
										shard.Put(op.Key, op.Value, op.ClientId, op.OpId)
										kv.lastOpId[op.ClientId] = op.OpId
										kv.saveShardKVState(false)
									}
								}
								go kv.applyWait.Trigger(result)
							case "Append":
								shardNum := key2shard(op.Key)
								latest := kv.configs[len(kv.configs)-1]
								switch {
								case latest.Shards[shardNum] != kv.gid:
									result.Result = ErrWrongGroup
								case kv.waitMigration.IsMigrationShard(shardNum):
									result.Result = ErrShardIsMigrating
								default:
									result.Result = OK
									if lastOpId, ok := kv.lastOpId[op.ClientId]; !ok || op.OpId > lastOpId {
										shard := kv.database.GetShard(shardNum)
										shard.Append(op.Key, op.Value, op.ClientId, op.OpId)
										kv.lastOpId[op.ClientId] = op.OpId
										kv.saveShardKVState(false)
									}
								}
								go kv.applyWait.Trigger(result)
							default:
								DPrintf("ShardKV %d (gid=%d) stateMachine received wrong opType OpArgs: %+v\n", kv.me, kv.gid, command)
							}
						case shardmaster.Config:
							newConfig := command
							oldConfig := kv.configs[len(kv.configs)-1]
							if newConfig.Num > oldConfig.Num && kv.waitMigration.IsEmpty() && kv.waitClean.IsEmpty() {
								kv.configs = append(kv.configs, newConfig)
								if oldConfig.Num > 0 {
									kv.waitMigration.Init(shardmaster.Config{
										Num:    newConfig.Num,
										Shards: oldConfig.Shards,
										Groups: oldConfig.Groups,
									})
									kv.waitClean.Init(newConfig)
									for shardNum := 0; shardNum < shardmaster.NShards; shardNum++ {
										oldGid := oldConfig.Shards[shardNum]
										newGid := newConfig.Shards[shardNum]
										if kv.gid == oldGid && kv.gid != newGid {
											// old shard remove from this group
											kv.waitClean.AddGidShard(newGid, shardNum)
										}
										if kv.gid != oldGid && kv.gid == newGid {
											// new shard assign to this group
											kv.waitMigration.AddGidShard(oldGid, shardNum)
										}
									}
									// remove shard from kv.Database and store in waitClean
									kv.waitClean.StoreCleanData(kv.database)
									if !kv.waitMigration.IsEmpty() {
										go kv.migrationHelper()
									}
									if !kv.waitClean.IsEmpty() {
										go kv.migrationChecker()
									}
								}
								DPrintf("ShardKV %d (gid=%d) is applying \nold Config = %+v \nnew Config = %+v\nkv.waitMigration:%+v\nkv.waitClean:%+v\n", kv.me,
									kv.gid, oldConfig, newConfig, kv.waitMigration, kv.waitClean)
								kv.configs = kv.configs[1:]
								kv.saveShardKVState(true)
							}
						case MigrateShardReply:
							if kv.waitMigration.GetConfig().Num == command.ConfigNum && command.Result == OK {
								if ok := kv.waitMigration.DeleteByGid(command.Gid); ok {
									for shardNum, shard := range command.Data {
										kv.database.SetShard(shardNum, shard)
									}
									str, _ := json.Marshal(kv.database)
									DPrintf("ShardKV %d (gid=%d) finished MigrateShard from (gid=%d) command= %+v database= %s", kv.me, kv.gid, command.Gid, command, str)
								}
							} else if kv.waitMigration.GetConfig().Num == command.ConfigNum && command.Result == ErrWaitMigrationTimeOut {
								if !kv.waitMigration.IsEmpty() {
									kv.waitMigration.Clear()
									DPrintf("ShardKV %d (gid=%d) waitMigration timeout (configNum %d)", kv.me, kv.gid, kv.waitClean.GetConfig().Num)
								}
							}
							kv.saveShardKVState(true)
						case CheckMigrateShardReply:
							if kv.waitClean.GetConfig().Num == command.ConfigNum && command.Result == OK {
								kv.waitClean.DeleteByGid(command.Gid)
							} else if kv.waitClean.GetConfig().Num == command.ConfigNum && command.Result == ErrWaitCleanTimeOut {
								if !kv.waitClean.IsEmpty() {
									kv.waitClean.Clear()
									DPrintf("ShardKV %d (gid=%d) waitClean timeout (configNum %d)", kv.me, kv.gid, kv.waitClean.GetConfig().Num)
								}
							}
							kv.saveShardKVState(true)
						default:
							DPrintf("ShardKV %d stateMachine received wrong type command %+v %v\n", kv.gid, applyMsg, reflect.TypeOf(applyMsg.Command))
						}
					}
					kv.mu.Unlock()
				}
			} else if command, ok := applyMsg.Command.(string); ok {
				if command == raft.CommandInstallSnapshot {
					DPrintf("ShardMaster %d stateMachine received InstallSnapshot %+v\n", kv.me, applyMsg)
					kv.init()
					kv.rf.Replay()
				}
			}
		}

	}
}
