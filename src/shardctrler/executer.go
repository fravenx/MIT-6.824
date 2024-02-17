package shardctrler

import "6.5840/raft"

func (sc *ShardCtrler) execute() {
	for {
		msg := <-sc.applyCh
		res := Err("")
		res = sc.applyMsg(msg)
		sc.reply(msg, res)
	}

}

func (sc *ShardCtrler) applyMsg(msg raft.ApplyMsg) Err {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	op := msg.Command.(Op)
	if op.IsQuery {
		clientId := op.ClientId
		if op.SeqNo > sc.table[clientId] {
			sc.table[clientId] = op.SeqNo
		}
		if op.Num > 0 && op.Num < len(sc.configs) {
			return OK
		}
		if msg.IsLeader && sc.table[clientId] == op.SeqNo {
			return OK
		}
		return ErrWrong
	} else if op.IsJoin {
		if sc.isRepeated(op.ClientId, op.SeqNo) {
			return ErrWrong
		}
		sc.doJoin(op)
		sc.table[op.ClientId] = op.SeqNo
		return OK
	} else if op.IsLeave {
		if sc.isRepeated(op.ClientId, op.SeqNo) {
			return ErrWrong
		}
		sc.doLeave(op)
		sc.table[op.ClientId] = op.SeqNo
		return OK
	} else if op.IsMove {
		if sc.isRepeated(op.ClientId, op.SeqNo) {
			return ErrWrong
		}
		sc.doMove(op)
		sc.table[op.ClientId] = op.SeqNo
		return OK

	}
	return ""
}

func (sc *ShardCtrler) reply(msg raft.ApplyMsg, res Err) {
	sc.mu.Lock()
	ch, ok := sc.waitCh[msg.CommandIndex]
	sc.mu.Unlock()
	if ok {
		if msg.IsLeader {
			select {
			case ch <- res:
			default:
			}
		} else {
			select {
			case ch <- ErrNotApplied:
			default:
			}
		}

	}
}
