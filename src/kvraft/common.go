package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	TimeOut        = "TimeOut"
	ErrWrong       = "ErrWrong"
)

type Err string

// Put or Append
type PutAppendArgs struct {
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

func cmpCommand(op1, op2 Op) bool {
	if op1.ClientId == op2.ClientId && op1.SeqNo == op2.SeqNo {
		return true
	}
	return false
}

func copyCommand(op1 Op) Op {
	op := Op{}
	if op1.IsGet {
		op.IsGet = true
		op.SeqNo = op1.SeqNo
		op.Key = op1.Key
		op.ClientId = op1.ClientId
	}

	if op1.IsPutAppend {
		op.IsPutAppend = true
		op.PutOrAppend = op1.PutOrAppend
		op.Value = op1.Value
		op.Key = op1.Key
		op.SeqNo = op1.SeqNo
		op.ClientId = op1.ClientId
	}
	return op
}
