package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.5840/labgob"
	"bytes"
	"math"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

const (
	LEADER   int = 1
	CADIDATE int = 2
	FOLLOWER int = 3
)

const (
	HEARTBEATINTERVAL int = 100
	ELECTIONTIMEOUT   int = 700
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Entry struct {
	Index   int
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu                 sync.Mutex          // Lock to protect shared access to this peer's state
	peers              []*labrpc.ClientEnd // RPC end points of all peers
	persister          *Persister          // Object to hold this peer's persisted state
	me                 int                 // this peer's index into peers[]
	dead               int32               // set by Kill()
	currentTerm        int
	votedFor           int
	state              int
	votes              map[int]bool
	electionTimer      time.Time
	heartbeatTimer     time.Time
	log                []Entry
	commitIndex        int
	lastApplied        int
	nextIndex          []int
	matchIndex         []int
	applyCh            chan ApplyMsg
	applyCond          *sync.Cond
	lastIncludedTerm   int
	lastIncludedIndex  int
	installSnapshotMsg ApplyMsg
	currentSnapshot    []byte
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == LEADER
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.persister.ReadSnapshot())
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var voteFor int
	var log []Entry
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		panic("decode err")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = voteFor
		rf.log = log
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		rf.lastApplied = lastIncludedIndex
		rf.commitIndex = lastIncludedIndex
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	k, _ := rf.getByIndex(index)
	rf.lastIncludedTerm = rf.log[k].Term
	rf.lastIncludedIndex = rf.log[k].Index
	rf.currentSnapshot = snapshot
	rf.log = rf.log[k:]
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, snapshot)
	if len(rf.log) > 0 {
		Debug(dSnap, "S%d snapshots to %d log[%d - %d]", rf.me, index, rf.log[0].Index, rf.log[len(rf.log)-1].Index)
	}
}

func (rf *Raft) SnapshotWithoutLock(index int, snapshot []byte) {
	k, _ := rf.getByIndex(index)
	rf.lastIncludedTerm = rf.log[k].Term
	rf.lastIncludedIndex = rf.log[k].Index
	rf.currentSnapshot = snapshot
	rf.log = rf.log[k:]
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, snapshot)
	if len(rf.log) > 0 {
		Debug(dSnap, "S%d snapshots to %d log[%d - %d]", rf.me, index, rf.log[0].Index, rf.log[len(rf.log)-1].Index)
	}
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntryArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntryReply struct {
	Term       int
	Success    bool
	FirstIndex int //  first index the follower stores for the term of the conficting entry
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Snapshot          []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Debug(dVote, "S%d Term %d gets Requestvote from Candidate %d Term %d", rf.me, rf.currentTerm, args.CandidateId, args.Term)
	if args.Term <= rf.currentTerm {
		Debug(dVote, "S%d reject to vote for S%d", rf.me, args.CandidateId)
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	rf.state = FOLLOWER
	rf.currentTerm = args.Term
	rf.votedFor = -1

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.checkLog(args.LastLogTerm, args.LastLogIndex) {
		Debug(dVote, "S%d vote for S%d", rf.me, args.CandidateId)
		Debug(dLog2, "S%d log = %v", rf.me, rf.log)
		rf.votedFor = args.CandidateId
		rf.resetElectionTimer()
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
	}
	rf.persist()

}

func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	rf.resetElectionTimer()
	needPersist := false
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = FOLLOWER
		needPersist = true
	}
	reply.Term = rf.currentTerm
	k, exists := rf.getByIndex(args.PrevLogIndex)
	if !exists {
		Debug(dLog2, "S%d Index%d not in its log", rf.me, args.PrevLogIndex)
		reply.Success = false
		reply.FirstIndex = -1
		if needPersist {
			rf.persist()
		}
		return

	}
	if rf.log[k].Term != args.PrevLogTerm {
		Debug(dLog2, "S%d term%d doesn't match at Index%d", rf.me, args.PrevLogTerm, args.PrevLogIndex)
		confictingTerm := rf.log[k].Term
		i := k
		for ; i > 0; i-- {
			if rf.log[i].Term != confictingTerm {
				break
			}
		}
		needPersist = true
		reply.Success = false
		reply.FirstIndex = i + 1
	} else {
		size := len(args.Entries)
		for i := 1; i <= size; i++ {
			Debug(dLog2, "S%d add %v int log", rf.me, args.Entries[i-1])
			if i+k < len(rf.log) && rf.log[i+k] != args.Entries[i-1] {
				rf.log = rf.log[:i+k]
			}
			if len(rf.log) <= i+k {
				rf.log = append(rf.log, args.Entries[i-1])
			}
			needPersist = true
		}
		if args.LeaderCommit > rf.commitIndex {
			indexOfLastNewEntry := rf.log[k+size].Index
			if indexOfLastNewEntry < args.LeaderCommit {
				rf.commitIndex = indexOfLastNewEntry
			} else {
				rf.commitIndex = args.LeaderCommit
			}
			if rf.commitIndex > rf.lastApplied {
				Debug(dLog2, "S%d update commitIndex to %d", rf.me, rf.commitIndex)
				rf.applyCond.Signal()
			}
		}
		reply.Success = true
	}
	if needPersist {
		rf.persist()
	}

}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm || args.LastIncludedIndex <= rf.commitIndex {
		reply.Term = rf.currentTerm
		return
	}
	rf.resetElectionTimer()
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = FOLLOWER
	}
	reply.Term = rf.currentTerm
	rf.log = []Entry{{Index: args.LastIncludedIndex, Term: args.LastIncludedTerm}}
	rf.commitIndex = args.LastIncludedIndex
	rf.lastApplied = args.LastIncludedIndex
	Debug(dTrace, "S%d getInstallSnapshot set lastApplied = %d ", rf.me, args.LastIncludedIndex)
	rf.installSnapshotMsg = ApplyMsg{CommandValid: false,
		Snapshot:      args.Snapshot,
		SnapshotValid: true,
		SnapshotIndex: args.LastIncludedIndex,
		SnapshotTerm:  args.LastIncludedTerm,
	}
	rf.applyCond.Signal()
	rf.SnapshotWithoutLock(args.LastIncludedIndex, args.Snapshot)

}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != LEADER {
		return -1, -1, false
	}
	entry := Entry{
		Index:   rf.lastLogIndex() + 1,
		Term:    rf.currentTerm,
		Command: command,
	}
	rf.log = append(rf.log, entry)
	rf.persist()
	index := entry.Index
	term := entry.Term
	Debug(dLog, "S%d append log %v at %d", rf.me, command, index)
	return index, term, true
}

func (rf *Raft) addNilCommand() {
	entry := Entry{
		Index:   rf.lastLogIndex() + 1,
		Term:    rf.currentTerm,
		Command: -1,
	}
	rf.log = append(rf.log, entry)
	rf.persist()
	index := entry.Index
	Debug(dLog, "S%d append nil command at %d", rf.me, index)
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) resetElectionTimer() {
	Debug(dVote, "S%d reset electionTimer ", rf.me)
	rf.electionTimer = time.Now()
}

func (rf *Raft) setHeartBeatTimer() {
	rf.heartbeatTimer = time.Now()
}

func (rf *Raft) lastLogIndex() int {
	return rf.log[len(rf.log)-1].Index
}

func (rf *Raft) lastLogTerm() int {
	return rf.log[len(rf.log)-1].Term

}

func (rf *Raft) checkLog(candidateTerm, candidateIndex int) bool {
	if rf.lastLogTerm() < candidateTerm {
		return true
	} else if rf.lastLogTerm() > candidateTerm {
		return false
	} else {
		return rf.lastLogIndex() <= candidateIndex
	}
}

func (rf *Raft) getByIndex(index int) (int, bool) {
	i, j := 0, len(rf.log)-1
	for i < j {
		mid := (i + j) >> 1
		if rf.log[mid].Index >= index {
			j = mid
		} else {
			i = mid + 1
		}
	}
	return i, i >= 0 && i < len(rf.log) && rf.log[i].Index == index
}

func (rf *Raft) initNextIndex() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		rf.nextIndex[i] = rf.lastLogIndex() + 1
	}
}

func (rf *Raft) initMatchIndex() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		rf.matchIndex[i] = 0
	}
}

func (rf *Raft) updateCommitIndex() {
	x := rf.commitIndex + 1
	for {
		_, exist := rf.getByIndex(x)
		if !exist {
			break
		}
		count := 1
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			if rf.matchIndex[i] >= x {
				count++
			}
		}
		if count > len(rf.peers)/2 {
			x++
		} else {
			break
		}
	}
	x--
	k, _ := rf.getByIndex(x)
	if rf.log[k].Term != rf.currentTerm {
		return
	}
	if x > rf.commitIndex {
		Debug(dLog, "S%d update commitIndex to %d", rf.me, x)
		rf.commitIndex = x
		rf.applyCond.Signal()
	}
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Check if a leader election should be started.
		rf.mu.Lock()
		timeDiff := time.Now().Sub(rf.electionTimer)
		if timeDiff < time.Duration(electionTime())*time.Millisecond || rf.state == LEADER {
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
			continue
		}
		Debug(dVote, "S%d electionTimer ", rf.me)
		rf.resetElectionTimer()
		rf.currentTerm++
		rf.votedFor = rf.me
		rf.persist()
		rf.state = CADIDATE
		currentTerm := rf.currentTerm
		lastLogIndex := rf.lastLogIndex()
		lastLogTerm := rf.lastLogTerm()
		rf.votes = make(map[int]bool)
		rf.votes[rf.me] = true
		rf.mu.Unlock()
		Debug(dVote, "S%d Term %d starts getting votes ", rf.me, currentTerm)
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			i := i
			go func() {
				args := RequestVoteArgs{}
				reply := RequestVoteReply{}
				args.Term = currentTerm
				args.CandidateId = rf.me
				args.LastLogIndex = lastLogIndex
				args.LastLogTerm = lastLogTerm
				Debug(dVote, "S%d sendRequestVote index = %d term = %d to S%d", rf.me, lastLogIndex, lastLogTerm, i)
				ok := rf.sendRequestVote(i, &args, &reply)
				if !ok {
					return
				}
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.currentTerm < reply.Term {
					rf.currentTerm = reply.Term
					rf.state = FOLLOWER
					rf.votedFor = -1
					rf.persist()
					return
				}
				if rf.currentTerm != args.Term || rf.state != CADIDATE {
					return
				}
				if reply.VoteGranted == false {

				} else {
					_, exists := rf.votes[i]
					oldlen := len(rf.votes)
					if !exists {
						rf.votes[i] = true
					}
					if oldlen <= len(rf.peers)/2 && len(rf.votes) > len(rf.peers)/2 {
						Debug(dVote, "S%d become leader log = %v", rf.me, rf.log)
						rf.state = LEADER
						//rf.addNilCommand() no need
						rf.initNextIndex()
						rf.initMatchIndex()
					}
				}
			}()
		}

	}
}

func (rf *Raft) heartbeat() {
	for rf.killed() == false {
		rf.mu.Lock()
		timeDiff := time.Now().Sub(rf.heartbeatTimer)
		if timeDiff < time.Duration(HEARTBEATINTERVAL)*time.Millisecond || rf.state != LEADER {
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
			continue
		}
		rf.setHeartBeatTimer()
		currentTerm := rf.currentTerm
		rf.mu.Unlock()
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			i := i
			go func() {
				args := AppendEntryArgs{}
				reply := AppendEntryReply{}
				args.Term = currentTerm
				args.LeaderId = rf.me
				rf.mu.Lock()

				if rf.nextIndex[i] <= rf.lastIncludedIndex {
					rf.mu.Unlock()
					go rf.installSnapshot(i)
					return
				}
				k, _ := rf.getByIndex(rf.nextIndex[i] - 1)
				if rf.lastLogIndex() >= rf.nextIndex[i] && rf.lastLogTerm() == rf.currentTerm {
					for j := k + 1; j < len(rf.log); j++ {
						args.Entries = append(args.Entries, rf.log[j])
					}
					Debug(dLog, "S%d sendAppendRpc pIndex = %d entries num = %d to S%d", rf.me, rf.log[k].Index, len(args.Entries), i)
				} else {
					Debug(dLog, "S%d sendHeartBeat pIndex = %d to S%d", rf.me, rf.log[k].Index, i)

				}
				args.PrevLogIndex = rf.log[k].Index
				args.PrevLogTerm = rf.log[k].Term
				args.LeaderCommit = rf.commitIndex
				rf.mu.Unlock()
				ok := rf.sendAppendEntry(i, &args, &reply)
				if !ok {
					return
				}
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.state = FOLLOWER
					rf.votedFor = -1
					rf.persist()
					return
				}
				if rf.currentTerm != args.Term {
					return
				}
				k, _ = rf.getByIndex(rf.nextIndex[i] - 1)
				if args.PrevLogIndex != rf.log[k].Index {
					return
				}
				if reply.Success == false {
					if reply.FirstIndex == -1 {
						j := k
						for j >= 0 && rf.log[j].Term == args.PrevLogTerm {
							j--
						}
						rf.nextIndex[i] = int(math.Min(float64(rf.nextIndex[i]), float64(j+1)))

					} else {
						rf.nextIndex[i] = int(math.Min(float64(reply.FirstIndex), float64(rf.nextIndex[i])))

					}
					Debug(dLog, "S%d set nextIndex[%d] to %d", rf.me, i, rf.nextIndex[i])
				} else {
					if args.PrevLogIndex+len(args.Entries) > rf.matchIndex[i] {
						rf.nextIndex[i] = args.PrevLogIndex + len(args.Entries) + 1
						rf.matchIndex[i] = args.PrevLogIndex + len(args.Entries)
						Debug(dLog, "S%d set matchIndex[%d] to %d", rf.me, i, rf.matchIndex[i])
						rf.updateCommitIndex()
					}
				}
			}()
		}

	}
}

func (rf *Raft) installSnapshot(i int) {
	rf.mu.Lock()
	args := InstallSnapshotArgs{}
	reply := InstallSnapshotReply{}
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.LastIncludedTerm = rf.lastIncludedTerm
	args.LastIncludedIndex = rf.lastIncludedIndex
	args.Snapshot = rf.currentSnapshot
	rf.mu.Unlock()
	Debug(dTrace, "S%d sendInstallSnapshot lastIndex = %d to S%d", rf.me, args.LastIncludedIndex, i)
	ok := rf.sendInstallSnapshot(i, &args, &reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
		rf.persist()
		return
	}
	if rf.currentTerm != args.Term {
		return
	}
	rf.nextIndex[i] = int(math.Max(float64(rf.nextIndex[i]), float64(args.LastIncludedIndex))) + 1
	rf.matchIndex[i] = rf.nextIndex[i] - 1

}

func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for rf.killed() == false {
		if rf.lastApplied < rf.commitIndex {
			for rf.lastApplied < rf.commitIndex {
				rf.lastApplied++
				index, _ := rf.getByIndex(rf.lastApplied)
				Debug(dCommit, "S%d apply %v", rf.me, rf.log[index])
				msg := ApplyMsg{
					CommandIndex: rf.lastApplied,
					Command:      rf.log[index].Command,
					CommandValid: true,
				}
				rf.mu.Unlock()
				rf.applyCh <- msg
				rf.mu.Lock()
			}
		} else if len(rf.installSnapshotMsg.Snapshot) > 0 {
			rf.mu.Unlock()
			rf.applyCh <- rf.installSnapshotMsg
			rf.mu.Lock()
			rf.installSnapshotMsg = ApplyMsg{}
		} else {
			rf.applyCond.Wait()
		}
	}

}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.electionTimer = time.Now()
	rf.heartbeatTimer = time.Now()
	rf.state = FOLLOWER
	rf.log = []Entry{{Index: 0, Term: 0, Command: 0}}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0
	rf.installSnapshotMsg = ApplyMsg{}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.currentSnapshot = persister.ReadSnapshot()
	Debug(dTrace, "S%d alive now ", rf.me)
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.heartbeat()
	go rf.applier()
	return rf
}
