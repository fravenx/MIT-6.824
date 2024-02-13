package shardctrler

import (
	"6.5840/raft"
	"sort"
	"time"
)
import "6.5840/labrpc"
import "sync"
import "6.5840/labgob"

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	table   map[int64]int64
	waitCh  map[int64]chan Op
	configs []Config // indexed by config num
}

type Op struct {
	IsQuery  bool
	Num      int
	Config   Config
	IsJoin   bool
	Servers  map[int][]string
	IsLeave  bool
	GIDs     []int
	IsMove   bool
	Shard    int
	GID      int
	ClientId int64
	SeqNo    int64
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	sc.mu.Lock()
	if args.SeqNo <= sc.table[args.ClientId] {
		reply.Err = OK
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()
	command := Op{}
	command.IsJoin = true
	command.Servers = args.Servers
	command.SeqNo = args.SeqNo
	command.ClientId = args.ClientId
	_, _, isLeader := sc.rf.Start(command)
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	sc.mu.Lock()
	ch := make(chan Op, 1)
	sc.waitCh[args.ClientId] = ch
	sc.mu.Unlock()

	select {
	case op := <-ch:
		sc.mu.Lock()
		delete(sc.waitCh, args.ClientId)
		sc.mu.Unlock()
		if cmpCommand(op, command) {
			reply.Err = OK
			return
		} else {
			reply.Err = ErrWrong
			return
		}
	case <-time.After(500 * time.Millisecond):
		sc.mu.Lock()
		delete(sc.waitCh, args.ClientId)
		sc.mu.Unlock()
		reply.Err = TimeOut
		return
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	sc.mu.Lock()
	if args.SeqNo <= sc.table[args.ClientId] {
		reply.Err = OK
		sc.mu.Unlock()
		return
	}

	sc.mu.Unlock()
	command := Op{}
	command.IsLeave = true
	command.GIDs = args.GIDs
	command.SeqNo = args.SeqNo
	command.ClientId = args.ClientId
	_, _, isLeader := sc.rf.Start(command)
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	sc.mu.Lock()
	ch := make(chan Op, 1)
	sc.waitCh[args.ClientId] = ch
	sc.mu.Unlock()

	select {
	case op := <-ch:
		sc.mu.Lock()
		delete(sc.waitCh, args.ClientId)
		sc.mu.Unlock()
		if cmpCommand(op, command) {
			reply.Err = OK
			return
		} else {
			reply.Err = ErrWrong
			return
		}
	case <-time.After(500 * time.Millisecond):
		sc.mu.Lock()
		delete(sc.waitCh, args.ClientId)
		sc.mu.Unlock()
		reply.Err = TimeOut
		return
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	sc.mu.Lock()
	if args.SeqNo <= sc.table[args.ClientId] {
		reply.Err = OK
		sc.mu.Unlock()
		return
	}

	sc.mu.Unlock()
	command := Op{}
	command.IsMove = true
	command.GID = args.GID
	command.Shard = args.Shard
	command.SeqNo = args.SeqNo
	command.ClientId = args.ClientId
	_, _, isLeader := sc.rf.Start(command)
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	sc.mu.Lock()
	ch := make(chan Op, 1)
	sc.waitCh[args.ClientId] = ch
	sc.mu.Unlock()

	select {
	case op := <-ch:
		sc.mu.Lock()
		delete(sc.waitCh, args.ClientId)
		sc.mu.Unlock()
		if cmpCommand(op, command) {
			reply.Err = OK
			return
		} else {
			reply.Err = ErrWrong
			return
		}
	case <-time.After(500 * time.Millisecond):
		sc.mu.Lock()
		delete(sc.waitCh, args.ClientId)
		sc.mu.Unlock()
		reply.Err = TimeOut
		return
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	sc.mu.Lock()
	if args.SeqNo <= sc.table[args.ClientId] {
		if args.Num == -1 || args.Num >= len(sc.configs) {
			reply.Config = sc.configs[len(sc.configs)-1]

		} else {
			reply.Config = sc.configs[args.Num]
		}
		reply.Err = OK
		sc.mu.Unlock()
		return
	}

	sc.mu.Unlock()
	command := Op{}
	command.IsQuery = true
	command.SeqNo = args.SeqNo
	command.ClientId = args.ClientId
	command.Num = args.Num
	_, _, isLeader := sc.rf.Start(command)
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	sc.mu.Lock()
	ch := make(chan Op, 1)
	sc.waitCh[args.ClientId] = ch
	sc.mu.Unlock()

	select {
	case op := <-ch:
		sc.mu.Lock()
		delete(sc.waitCh, args.ClientId)
		sc.mu.Unlock()
		if cmpCommand(op, command) {
			reply.Config = op.Config
			reply.Err = OK
			return
		} else {
			reply.Err = ErrWrong
			return
		}
	case <-time.After(500 * time.Millisecond):
		sc.mu.Lock()
		delete(sc.waitCh, args.ClientId)
		sc.mu.Unlock()
		reply.Err = TimeOut
		return
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) doJoin(op Op) {
	println("doJoin %v at S%d", op.Servers, sc.me)
	c := copyConfig(sc.configs[len(sc.configs)-1])
	c.Num++
	for k, v := range op.Servers {
		c.Groups[k] = v
	}
	cid := make(map[int][]int) // gid -> shard
	var gs []int
	for gid := range c.Groups {
		cid[gid] = []int{}
		gs = append(gs, gid)
	}
	sort.Ints(gs)
	for i, v := range c.Shards {
		cid[v] = append(cid[v], i)
	}

	maxi, maxv, mini, minv := maxmininMap(cid, gs)
	for maxv-minv > 1 {
		size := (maxv - minv) / 2
		arr := cid[maxi][:size]
		cid[mini] = append(cid[mini], arr...)
		cid[maxi] = cid[maxi][size:]
		for _, v := range arr {
			c.Shards[v] = mini
		}
		maxi, maxv, mini, minv = maxmininMap(cid, gs)

	}
	sc.configs = append(sc.configs, c)
}

func (sc *ShardCtrler) doLeave(op Op) {
	Debug(dTrace, "doLeave %v at S%d", op.GIDs, sc.me)
	println("doLeave %v at S%d", op.GIDs, sc.me)

	c := copyConfig(sc.configs[len(sc.configs)-1])
	c.Num++
	for _, v := range op.GIDs {
		for shard, gid := range c.Shards {
			if gid == v {
				c.Shards[shard] = 0
			}
		}
		delete(c.Groups, v)
	}

	cid := make(map[int][]int) // gid -> shard
	var gs []int
	for gid := range c.Groups {
		cid[gid] = []int{}
		gs = append(gs, gid)
	}
	sort.Ints(gs)
	for i, v := range c.Shards {
		cid[v] = append(cid[v], i)
	}

	maxi, maxv, mini, minv := maxmininMap(cid, gs)
	for maxv-minv > 1 {
		size := (maxv - minv) / 2
		arr := cid[maxi][:size]
		cid[mini] = append(cid[mini], arr...)
		cid[maxi] = cid[maxi][size:]
		for _, v := range arr {
			c.Shards[v] = mini
		}
		maxi, maxv, mini, minv = maxmininMap(cid, gs)

	}
	sc.configs = append(sc.configs, c)
}

func (sc *ShardCtrler) doMove(op Op) {
	println("doMove shard = %d,gid = %d at S%d\n", op.Shard, op.GID, sc.me)
	c := copyConfig(sc.configs[len(sc.configs)-1])
	c.Num++
	for i, _ := range c.Shards {
		if i == op.Shard {
			c.Shards[i] = op.GID
		}
	}
	sc.configs = append(sc.configs, c)
}

func (sc *ShardCtrler) isRepeated(clientId, seqNo int64) bool {
	value, ok := sc.table[clientId]
	if ok && value >= seqNo {
		return true
	}
	return false
}

func (sc *ShardCtrler) execute() {
	for {
		msg := <-sc.applyCh
		op := msg.Command.(Op)
		sc.mu.Lock()
		if op.IsQuery {
			clientId := op.ClientId
			if op.SeqNo > sc.table[clientId] {
				sc.table[clientId] = op.SeqNo
			}
			_, ok := sc.waitCh[clientId]
			if ok && sc.table[clientId] == op.SeqNo {
				if op.Num == -1 || op.Num >= len(sc.configs) {
					op.Config = sc.configs[len(sc.configs)-1]

				} else {
					op.Config = sc.configs[op.Num]
				}
				select {
				case sc.waitCh[clientId] <- op:
				default:
				}
			}
		} else if op.IsJoin {
			if sc.isRepeated(op.ClientId, op.SeqNo) {
				sc.mu.Unlock()
				continue
			}
			sc.doJoin(op)
			sc.table[op.ClientId] = op.SeqNo
			_, ok := sc.waitCh[op.ClientId]
			if ok {
				select {
				case sc.waitCh[op.ClientId] <- op:
				default:
				}
			}
		} else if op.IsLeave {
			if sc.isRepeated(op.ClientId, op.SeqNo) {
				sc.mu.Unlock()
				continue
			}
			sc.doLeave(op)
			sc.table[op.ClientId] = op.SeqNo
			_, ok := sc.waitCh[op.ClientId]
			if ok {
				select {
				case sc.waitCh[op.ClientId] <- op:
				default:
				}
			}

		} else if op.IsMove {
			if sc.isRepeated(op.ClientId, op.SeqNo) {
				sc.mu.Unlock()
				continue
			}
			sc.doMove(op)
			sc.table[op.ClientId] = op.SeqNo
			_, ok := sc.waitCh[op.ClientId]
			if ok {
				select {
				case sc.waitCh[op.ClientId] <- op:
				default:
				}
			}

		}

		sc.mu.Unlock()

	}

}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	sc.table = make(map[int64]int64)
	sc.waitCh = make(map[int64]chan Op)
	go sc.execute()
	return sc
}
