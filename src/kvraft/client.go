package kvraft

import (
	"6.5840/labrpc"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers  []*labrpc.ClientEnd
	leader   int64
	clientId int64
	seq      int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.leader = 0
	ck.clientId = nrand()
	ck.seq = 1
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	Debug(dInfo, "C%d sendGET seq = %d to S%d", ck.clientId, ck.seq, ck.leader)
	args := GetArgs{}
	reply := GetReply{}
	args.Key = key
	args.ClientId = ck.clientId
	args.SeqNo = ck.seq
	for {
		ok := ck.servers[ck.leader].Call("KVServer.Get", &args, &reply)
		if reply.Err == OK {
			Debug(dInfo, "C%d GET seq = %d completes", ck.clientId, ck.seq, ck.leader)
			ck.seq++
			return reply.Value
		} else {
			if !ok || reply.Err == ErrWrongLeader || reply.Err == TimeOut {
				Debug(dInfo, "C%d GET seq = %d pick another leader", ck.clientId, ck.seq)
				ck.leader = (ck.leader + 1) % int64(len(ck.servers))
			}
			time.Sleep(20 * time.Millisecond)
		}
	}

}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{}
	reply := PutAppendReply{}
	args.Key = key
	args.Value = value
	args.Op = op
	args.ClientId = ck.clientId
	args.SeqNo = ck.seq
	for {
		ok := ck.servers[ck.leader].Call("KVServer.PutAppend", &args, &reply)
		if reply.Err == OK {
			Debug(dInfo, "C%d PUT seq = %d completes", ck.clientId, ck.seq)
			ck.seq++
			return
		} else {
			if !ok || reply.Err == ErrWrongLeader || reply.Err == TimeOut {
				ck.leader = (ck.leader + 1) % int64(len(ck.servers))
			}
			time.Sleep(20 * time.Millisecond)
		}
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
