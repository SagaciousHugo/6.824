package shardkv

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	reply.Result, reply.Value = kv.start(*args)
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	reply.Result, _ = kv.start(*args)
}

func (kv *ShardKV) MigrateShard(args *MigrateShardArgs, reply *MigrateShardReply) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	latestConfig := kv.configs[len(kv.configs)-1]
	reply.ConfigNum = args.ConfigNum
	reply.Gid = kv.gid
	if args.ConfigNum > latestConfig.Num {
		reply.Result = ErrNotReceiveNewConf
		return
	} else if args.ConfigNum != latestConfig.Num {
		reply.Result = ErrShardHasBeenCleaned
		return
	}
	reply.Data = make(map[int]Shard)
	for _, shardNum := range args.ShardNums {
		if shard, ok := kv.waitClean.GetShard(shardNum); ok {
			reply.Data[shardNum] = shard
		} else {
			reply.Result = ErrShardHasBeenCleaned
			return
		}
	}
	reply.Result = OK
}

func (kv *ShardKV) CheckMigrateShard(args *CheckMigrateShardArgs, reply *CheckMigrateShardReply) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	latestConfig := kv.configs[len(kv.configs)-1]
	reply.ConfigNum = args.ConfigNum
	reply.Gid = kv.gid
	if args.ConfigNum > latestConfig.Num {
		reply.Result = ErrNotReceiveNewConf
		return
	} else if args.ConfigNum < latestConfig.Num {
		reply.Result = OK
		return
	}
	for _, shardNum := range args.ShardNums {
		if ok := kv.waitMigration.IsMigrationShard(shardNum); ok {
			reply.Result = ErrNewConfIsApplying
			return
		}
	}
	reply.Result = OK
}
