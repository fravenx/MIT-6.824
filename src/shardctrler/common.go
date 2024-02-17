package shardctrler

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

const (
	OK            = "OK"
	TimeOut       = "TimeOut"
	ErrWrong      = "ErrWrong"
	ErrNotApplied = "ErrNotApplied"
)

type Err string

type JoinArgs struct {
	Servers  map[int][]string // new GID -> servers mappings
	ClientId int64
	SeqNo    int64
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs     []int
	ClientId int64
	SeqNo    int64
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard    int
	GID      int
	ClientId int64
	SeqNo    int64
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num      int // desired config number
	ClientId int64
	SeqNo    int64
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}

func cmpCommand(op1, op2 Op) bool {
	if op1.ClientId == op2.ClientId && op1.SeqNo == op2.SeqNo {
		return true
	}
	return false
}

func copyConfig(config Config) Config {
	cp := Config{}
	cp.Num = config.Num
	cp.Shards = [10]int{}
	for i, v := range config.Shards {
		cp.Shards[i] = v
	}
	cp.Groups = map[int][]string{}
	for key, val := range config.Groups {
		newSlice := make([]string, len(val))
		for i, v := range val {
			newSlice[i] = v
		}
		cp.Groups[key] = newSlice
	}
	return cp
}

func maxmininMap(m map[int][]int, gs []int) (int, int, int, int) {
	maxv := -1
	maxi := 0
	minv := int(1e8)
	mini := 0
	for _, gid := range gs {
		l := len(m[gid])
		if l > maxv {
			maxv = l
			maxi = gid
		}
		if l < minv {
			minv = l
			mini = gid
		}
	}

	if len(m[0]) > 0 {
		if minv != int(1e8) {
			return 0, len(m[0]) * 2, mini, 0
		} else {
			return 0, 0, 0, 0
		}
	}
	return maxi, maxv, mini, minv
}
