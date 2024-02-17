package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK               = "OK"
	ErrWrongGroup    = "ErrWrongGroup"
	ErrWrongLeader   = "ErrWrongLeader"
	ErrNotApplied    = "ErrNotApplied"
	TimeOut          = "Timeout"
	ErrSHARDNOTREADY = "ShardNotReady"
	ErrRepeated      = "ErrRepeated"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key      string
	Value    string
	Op       string // "Put" or "Append"
	ClientId int64
	SeqNo    int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key      string
	ClientId int64
	SeqNo    int64
}

type GetReply struct {
	Err   Err
	Value string
}

type PushShardArgs struct {
	Num    int
	Data   map[string]string
	Shards []int
	Table  map[int64]int64
	Dst    []string
	Src    []string
}

type PushShardReply struct {
	Err Err
}

type DeleteShardArgs struct {
	Num    int
	Keys   []string
	Shards []int
	Dst    []string
}

type DeleteShardReply struct {
	Err Err
}

func copyTable(m1 map[int64]int64) map[int64]int64 {
	res := make(map[int64]int64)
	for k, v := range m1 {
		res[k] = v
	}
	return res

}
