package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"sync"
	"sync/atomic"
	"time"
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

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	table        map[int64]int64
	data         map[string]string
	waitCh       map[int64]chan Op
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	if args.SeqNo <= kv.table[args.ClientId] {
		v := kv.data[args.Key]
		reply.Value = v
		reply.Err = OK
		kv.mu.Unlock()
		return
	}

	kv.mu.Unlock()
	command := Op{}
	command.IsGet = true
	command.SeqNo = args.SeqNo
	command.ClientId = args.ClientId
	command.Key = args.Key
	_, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		Debug(dTrace, "GET C%d seq %d wrong leader at S%d", args.ClientId, args.SeqNo, kv.me)
		return
	}

	kv.mu.Lock()
	ch := make(chan Op, 1)
	kv.waitCh[args.ClientId] = ch
	kv.mu.Unlock()

	select {
	case op := <-ch:
		kv.mu.Lock()
		delete(kv.waitCh, args.ClientId)
		kv.mu.Unlock()
		if cmpCommand(op, command) {
			reply.Value = op.Value
			reply.Err = OK
			Debug(dTrace, "GET C%d seq %d return at S%d", args.ClientId, args.SeqNo, kv.me)
			return
		} else {
			reply.Err = ErrWrong
			Debug(dTrace, "GET C%d seq %d ErrWrong at S%d", args.ClientId, args.SeqNo, kv.me)
			return
		}
	case <-time.After(500 * time.Millisecond):
		kv.mu.Lock()
		delete(kv.waitCh, args.ClientId)
		kv.mu.Unlock()
		reply.Err = TimeOut
		Debug(dTrace, "GET C%d seq %d TimeOut at S%d", args.ClientId, args.SeqNo, kv.me)
		return
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	Debug(dTrace, "PUT C%d seq %d arrive at S%d", args.ClientId, args.SeqNo, kv.me)
	kv.mu.Lock()
	if args.SeqNo <= kv.table[args.ClientId] {
		reply.Err = OK
		Debug(dTrace, "PUT C%d seq %d aready done at S%d", args.ClientId, args.SeqNo, kv.me)
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

	_, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	ch := make(chan Op, 1)
	kv.waitCh[args.ClientId] = ch
	kv.mu.Unlock()

	select {
	case op := <-ch:
		kv.mu.Lock()
		delete(kv.waitCh, args.ClientId)
		kv.mu.Unlock()
		if cmpCommand(op, command) {
			Debug(dTrace, "PUT C%d seq %d return at S%d", args.ClientId, args.SeqNo, kv.me)
			reply.Err = OK
			return
		} else {
			Debug(dTrace, "PUT C%d seq %d wrong at S%d", args.ClientId, args.SeqNo, kv.me)
			reply.Err = ErrWrong
			return
		}
	case <-time.After(500 * time.Millisecond):
		kv.mu.Lock()
		delete(kv.waitCh, args.ClientId)
		kv.mu.Unlock()
		Debug(dTrace, "PUT C%d seq %d timeout at S%d", args.ClientId, args.SeqNo, kv.me)
		reply.Err = TimeOut
		return
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) isRepeated(clientId, seqNo int64) bool {
	value, ok := kv.table[clientId]
	if ok && value >= seqNo {
		return true
	}
	return false
}

func (kv *KVServer) execute() {
	for kv.killed() == false {
		msg := <-kv.applyCh
		Debug(dTrace, "C%d seq %d execute() at S%d", msg.Command.(Op).ClientId, msg.Command.(Op).SeqNo, kv.me)
		if msg.CommandValid {
			op := msg.Command.(Op)
			kv.mu.Lock()
			if op.IsGet {
				clientId := op.ClientId
				if op.SeqNo > kv.table[clientId] {
					kv.table[clientId] = op.SeqNo
				}
				_, ok := kv.waitCh[clientId]
				if ok && kv.table[clientId] == op.SeqNo {
					op.Value = kv.data[op.Key]
					select {
					case kv.waitCh[clientId] <- op:
					default:
					}
				}
			} else {
				key := op.Key
				value := op.Value
				clientId := op.ClientId
				if kv.isRepeated(op.ClientId, op.SeqNo) {
					kv.mu.Unlock()
					continue
				}
				Debug(dTrace, "C%d seq %d applied at S%d", op.ClientId, op.SeqNo, kv.me)
				if op.PutOrAppend == "Put" {
					kv.data[key] = value
				} else {
					kv.data[key] = kv.data[key] + value
				}
				kv.table[clientId] = op.SeqNo
				_, ok := kv.waitCh[clientId]
				if ok && kv.table[clientId] == op.SeqNo {
					select {
					case kv.waitCh[clientId] <- op:
					default:
					}
				}
			}
			kv.mu.Unlock()
		}

	}

}

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
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.table = make(map[int64]int64)
	kv.data = make(map[string]string)
	kv.waitCh = make(map[int64]chan Op)
	go kv.execute()
	return kv
}
