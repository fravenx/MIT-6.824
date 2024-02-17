package shardkv

import (
	"6.5840/labrpc"
	"6.5840/shardctrler"
	"bytes"
	"sync/atomic"
	"time"
)
import "6.5840/raft"
import "sync"
import "6.5840/labgob"

const (
	WORKING int = 0
	MISSING int = 1
	ADDING  int = 2
)

const (
	UPDATECONFIG = 100 * time.Millisecond
	SENDSHARDS   = 150 * time.Millisecond
)

type Op struct {
	IsGet       bool
	IsPutAppend bool
	ClientId    int64
	SeqNo       int64
	Key         string
	Value       string
	PutOrAppend string
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	mck          *shardctrler.Clerk
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	persister   *raft.Persister
	table       map[int64]int64
	data        map[string]string
	waitCh      map[int]chan Err
	dead        int32 // set by Kill()
	lastConfig  shardctrler.Config
	config      shardctrler.Config
	shardsState map[int]int
	bytes       int
}

func (kv *ShardKV) checkShard(str string) (bool, bool) {
	shard := key2shard(str)
	if kv.config.Shards[shard] != kv.gid {
		return false, true
	}
	if kv.shardsState[shard] != WORKING {
		return true, false
	}
	return true, true
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	//if kv.isLeader() == false {
	//	reply.Err = ErrWrongLeader
	//	return
	//}
	kv.mu.Lock()
	//if args.SeqNo <= kv.table[args.ClientId] {
	//	v := kv.data[args.Key]
	//	reply.Value = v
	//	reply.Err = OK
	//	kv.mu.Unlock()
	//	return
	//}
	if shardMatch, shardReady := kv.checkShard(args.Key); !shardMatch || !shardReady {
		if !shardMatch {
			reply.Err = ErrWrongGroup
		} else {
			reply.Err = ErrSHARDNOTREADY
		}
		kv.mu.Unlock()
		return
	}

	kv.mu.Unlock()
	command := Op{}
	command.IsGet = true
	command.SeqNo = args.SeqNo
	command.ClientId = args.ClientId
	command.Key = args.Key
	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		Debug(dTrace, "GET C%d seq %d wrong leader at S%d", args.ClientId, args.SeqNo, kv.me)
		return
	}
	ch := make(chan Err, 1)
	kv.mu.Lock()
	kv.waitCh[index] = ch
	kv.mu.Unlock()

	select {
	case str := <-ch:
		if str == OK {
			kv.mu.Lock()
			reply.Value = kv.data[args.Key]
			kv.mu.Unlock()
			reply.Err = OK
			return
		}
		reply.Err = str
		return

	case <-time.After(100 * time.Millisecond):
		reply.Err = TimeOut
		Debug(dTrace, "GET C%d seq %d TimeOut at S%d", args.ClientId, args.SeqNo, kv.me)
		return
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	Debug(dTrace, "PUT C%d seq %d arrive at S%d", args.ClientId, args.SeqNo, kv.me)
	//if kv.isLeader() == false {
	//	reply.Err = ErrWrongLeader
	//	return
	//}
	kv.mu.Lock()
	if args.SeqNo <= kv.table[args.ClientId] {
		reply.Err = OK
		Debug(dTrace, "PUT C%d seq %d aready done at S%d", args.ClientId, args.SeqNo, kv.me)
		kv.mu.Unlock()
		return
	}
	if shardMatch, shardReady := kv.checkShard(args.Key); !shardMatch || !shardReady {
		if !shardMatch {
			reply.Err = ErrWrongGroup
		} else {
			reply.Err = ErrSHARDNOTREADY
		}
		kv.mu.Unlock()
		return
	}

	kv.mu.Unlock()
	command := Op{}
	command.IsPutAppend = true
	command.SeqNo = args.SeqNo
	command.ClientId = args.ClientId
	command.Key = args.Key
	command.PutOrAppend = args.Op
	command.Value = args.Value

	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	ch := make(chan Err, 1)
	kv.mu.Lock()
	kv.waitCh[index] = ch
	kv.mu.Unlock()

	select {
	case res := <-ch:
		reply.Err = res
		return

	case <-time.After(100 * time.Millisecond):
		Debug(dTrace, "PUT C%d seq %d timeout at S%d", args.ClientId, args.SeqNo, kv.me)
		reply.Err = TimeOut
		return
	}
}

func (kv *ShardKV) PushShard(args *PushShardArgs, reply *PushShardReply) {
	if kv.isLeader() == false {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	if kv.config.Num < args.Num {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}

	if kv.config.Num > args.Num {
		reply.Err = OK
		kv.mu.Unlock()
		go kv.sendDeleteShardRPC(args)
		return
	}
	if kv.config.Num == args.Num {
		pushed := false
		for _, shard := range args.Shards {
			if kv.shardsState[shard] == WORKING {
				pushed = true
			}
		}
		if pushed {
			reply.Err = OK
			kv.mu.Unlock()
			go kv.sendDeleteShardRPC(args)
			return
		}
	}
	kv.mu.Unlock()
	index, _, isLeader := kv.rf.Start(*args)

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	ch := make(chan Err, 1)
	kv.mu.Lock()
	kv.waitCh[index] = ch
	kv.mu.Unlock()

	select {
	case res := <-ch:
		if res == OK {
			reply.Err = res
			go kv.sendDeleteShardRPC(args)
			return
		} else {
			reply.Err = res
			return
		}
	case <-time.After(50 * time.Millisecond):
		reply.Err = TimeOut
		return
	}
}

func (kv *ShardKV) DeleteShard(args *DeleteShardArgs, reply *DeleteShardReply) {
	if kv.isLeader() == false {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	if kv.config.Num > args.Num {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}

	if kv.config.Num == args.Num {
		deleted := false
		for _, shard := range args.Shards {
			if kv.shardsState[shard] == WORKING {
				deleted = true
			}
		}
		if deleted {
			reply.Err = OK
			kv.mu.Unlock()
			return
		}
	}
	kv.mu.Unlock()
	index, _, isLeader := kv.rf.Start(*args)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	ch := make(chan Err, 1)
	kv.mu.Lock()
	kv.waitCh[index] = ch
	kv.mu.Unlock()

	select {
	case res := <-ch:
		reply.Err = res
		return
	case <-time.After(50 * time.Millisecond):
		reply.Err = TimeOut
		return
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) isRepeated(clientId, seqNo int64) bool {
	value, ok := kv.table[clientId]
	if ok && value >= seqNo {
		return true
	}
	return false
}

func (kv *ShardKV) isLeader() bool {
	_, isleader := kv.rf.GetState()
	return isleader
}

func (kv *ShardKV) readyForNewConfig() bool {
	for _, v := range kv.shardsState {
		if v != WORKING {
			return false
		}
	}
	return true
}

func (kv *ShardKV) updateConfig() {
	for kv.killed() == false {
		kv.mu.Lock()
		if !kv.readyForNewConfig() || !kv.isLeader() {
			kv.mu.Unlock()
			time.Sleep(UPDATECONFIG)
			continue
		}
		configNum := kv.config.Num
		kv.mu.Unlock()
		go func() {
			config := kv.mck.Query(configNum + 1)
			if config.Num > configNum {
				kv.rf.Start(config)
			}
		}()
		time.Sleep(UPDATECONFIG)

	}
}

func (kv *ShardKV) updateShardsState() {
	if kv.lastConfig.Num == 0 {
		return
	}

	for shard, gid := range kv.config.Shards {
		lastGid := kv.lastConfig.Shards[shard]
		if lastGid == kv.gid && gid != kv.gid {
			kv.shardsState[shard] = MISSING
		} else if lastGid != 0 && lastGid != kv.gid && gid == kv.gid {
			kv.shardsState[shard] = ADDING
		}
	}
}

func (kv *ShardKV) hasMissingShards() bool {
	for _, v := range kv.shardsState {
		if v == MISSING {
			return true
		}
	}
	return false
}

func (kv *ShardKV) preparePushShardArgs() []PushShardArgs {
	gs := make(map[int][]int)
	for shard, gid := range kv.config.Shards {
		lastGid := kv.lastConfig.Shards[shard]
		if lastGid == kv.gid && gid != kv.gid {
			gs[gid] = append(gs[gid], shard)
		}
	}

	res := make([]PushShardArgs, 0)
	for gid, shards := range gs {
		shardskv := make(map[string]string)
		hash := make(map[int]bool)
		for _, shard := range shards {
			hash[shard] = true
		}
		for k, v := range kv.data {
			if hash[key2shard(k)] == true {
				shardskv[k] = v
			}
		}
		args := PushShardArgs{}
		args.Shards = shards
		args.Num = kv.config.Num
		args.Data = shardskv
		args.Src = kv.lastConfig.Groups[kv.gid]
		args.Table = copyTable(kv.table)
		args.Dst = kv.config.Groups[gid]
		res = append(res, args)
	}
	return res
}

func (kv *ShardKV) sendPushShardRPC(args PushShardArgs) {
	for _, server := range args.Dst {
		srv := kv.make_end(server)
		var reply PushShardReply
		ok := srv.Call("ShardKV.PushShard", &args, &reply)
		if ok && (reply.Err == OK) {
			return
		}
	}

}

func (kv *ShardKV) sendDeleteShardRPC(args1 *PushShardArgs) {
	args := DeleteShardArgs{}
	args.Shards = args1.Shards
	args.Dst = args1.Src
	args.Num = args1.Num
	args.Keys = make([]string, 0)
	for k, _ := range args1.Data {
		args.Keys = append(args.Keys, k)
	}
	for _, server := range args.Dst {
		srv := kv.make_end(server)
		var reply DeleteShardReply
		ok := srv.Call("ShardKV.DeleteShard", &args, &reply)
		if ok && (reply.Err == OK) {
			return
		}
	}

}

func (kv *ShardKV) sendShards() {
	for kv.killed() == false {
		kv.mu.Lock()
		if !kv.hasMissingShards() || !kv.isLeader() {
			kv.mu.Unlock()
			time.Sleep(SENDSHARDS)
			continue
		}
		argsarr := kv.preparePushShardArgs()
		kv.mu.Unlock()
		for _, args := range argsarr {
			go kv.sendPushShardRPC(args)
		}
		time.Sleep(SENDSHARDS)
	}
}

func (kv *ShardKV) getSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.table)
	e.Encode(kv.data)
	e.Encode(kv.lastConfig)
	e.Encode(kv.config)
	e.Encode(kv.shardsState)

	snapshot := w.Bytes()
	return snapshot
}

func (kv *ShardKV) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var table map[int64]int64
	var data2 map[string]string
	var lastConfig shardctrler.Config
	var config1 shardctrler.Config
	var shardsState map[int]int
	if d.Decode(&table) != nil ||
		d.Decode(&data2) != nil ||
		d.Decode(&lastConfig) != nil ||
		d.Decode(&config1) != nil ||
		d.Decode(&shardsState) != nil {
		panic("decode err")
	} else {
		kv.table = table
		kv.data = data2
		kv.lastConfig = lastConfig
		kv.config = config1
		kv.shardsState = shardsState
	}
}

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
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(PushShardArgs{})
	labgob.Register(DeleteShardArgs{})
	labgob.Register(shardctrler.Config{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.table = make(map[int64]int64)
	kv.data = make(map[string]string)
	kv.waitCh = make(map[int]chan Err)
	kv.lastConfig = shardctrler.Config{}

	kv.config = shardctrler.Config{}
	kv.shardsState = make(map[int]int)
	for i := 0; i < 10; i++ {
		kv.shardsState[i] = 0
	}
	kv.persister = persister
	kv.readSnapshot(persister.ReadSnapshot())
	go kv.execute()
	go kv.updateConfig()
	go kv.sendShards()
	return kv
}
