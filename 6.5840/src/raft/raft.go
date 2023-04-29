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
	//	"bytes"

	"bytes"
	"log"
	"math"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
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
type LogEntry struct {
	Command  interface{}
	Log_Term int
	Index    int
}
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int
	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Status int16

const (
	Follower  = 0
	Candidate = 1
	Leader    = 2
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	// Your data here (2A, 2B, 2C).
	Raft_Status   Status
	CurrentTerm   int
	VoteFor       int
	Log_Array     []LogEntry
	Committed_Idx int
	ApplyChan     chan ApplyMsg
	// ApplyChanTemp    chan ApplyMsg
	Last_Applied_Idx int
	// leader violate and should reinitilize in the start of vote
	Next_Idx         []int
	Match_Idx        []int
	HeartBeatTimeOut time.Time
	ElectionTimeOut  time.Time
	HeartTime        int
	ElectTime        int
	ApplyCond        sync.Cond
	ReplicatorCond   []*sync.Cond
	seq              uint64
	lastIncludeIdx   int
	lastIncludeTerm  int
	//add
	snap_lock    sync.Mutex
	snapshot_idx int

	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.CurrentTerm
	isleader = rf.Raft_Status == Leader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) ReadSnapshot() []byte {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.ReadSnapshot()
}

func (rf *Raft) Persist(first interface{}, second interface{}) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(first)
	e.Encode(second)
	snapshot := w.Bytes()
	rf.persister.Save(rf.persister.ReadRaftState(), snapshot)
}
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.Log_Array)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VoteFor)
	e.Encode(rf.lastIncludeIdx)
	e.Encode(rf.lastIncludeTerm)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.persister.ReadSnapshot())
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var log []LogEntry
	var current_term int
	var vote_for int
	var lastIncludeIdx int
	var lastIncludeTerm int
	if d.Decode(&log) != nil || d.Decode(&current_term) != nil || d.Decode(&vote_for) != nil || d.Decode(&lastIncludeIdx) != nil || d.Decode(&lastIncludeTerm) != nil {
		DPrintf("Node{%v} failed readPersist", rf.me)
	} else {
		rf.Log_Array = log
		rf.CurrentTerm = current_term
		rf.VoteFor = vote_for
		rf.lastIncludeIdx = lastIncludeIdx
		rf.lastIncludeTerm = lastIncludeTerm
	}
}

type SnapArgs struct {
	Leader_Term       int
	Leader_Id         int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}
type SnapReply struct {
	Term              int
	Success           bool
	LastIncludedIndex int
	LastIncludedTerm  int
}

func (rf *Raft) sendSnapshot(server int, args *SnapArgs, reply *SnapReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) InstallSnapshot(args *SnapArgs, reply *SnapReply) {
	DPrintf("install rpc Node{%v} 1", rf.me)
	rf.mu.Lock()
	// defer rf.mu.Unlock()
	DPrintf("install rpc Node{%v}", rf.me)
	reply.Term = rf.CurrentTerm
	// my add
	reply.LastIncludedIndex = args.LastIncludedIndex
	if rf.CurrentTerm > args.Leader_Term {
		DPrintf("InstallSnapshot node{%v} term{%v} large leader_term{%v} id{%v}", rf.me, rf.CurrentTerm, args.Leader_Term, args.Leader_Id)
		rf.mu.Unlock()
		return
	}
	rf.ToFollower()
	rf.ResetElection()
	if rf.CurrentTerm < args.Leader_Term {
		rf.CurrentTerm = args.Leader_Term
		rf.VoteFor = args.Leader_Id
		rf.persist()
	}
	if rf.Committed_Idx >= args.LastIncludedIndex {
		DPrintf("InstallSnapshot commit{%v} >= lastidx{%v} node{%v} term{%v} eader_term{%v} id{%v}", rf.Committed_Idx, args.LastIncludedIndex, rf.me, rf.CurrentTerm, args.Leader_Term, args.Leader_Id)
		rf.mu.Unlock()
		return
	}
	// may be
	// if args.Data == nil {
	// 	DPrintf("Node{%v} data nil", rf.me)
	// 	return
	// }
	// DPrintf("Node{%v} snapshot index{%v} snapshot{%v}", rf.me, index, snapshot)
	if rf.lastIncludeIdx >= args.LastIncludedIndex {
		DPrintf("install snapshot invaild index{%v} preindex{%v} term{%v}", args.LastIncludedIndex, rf.lastIncludeIdx, rf.CurrentTerm)
		rf.mu.Unlock()
		return
	}
	snapshot_array := make([]LogEntry, 1)
	snapshot_array[0].Log_Term = args.LastIncludedTerm
	if args.LastIncludedIndex < rf.GetLastLog().Index {
		snapshot_array = append(snapshot_array, rf.Log_Array[args.LastIncludedIndex+1-rf.lastIncludeIdx:]...)
	}
	rf.Log_Array = snapshot_array
	rf.lastIncludeIdx = args.LastIncludedIndex
	rf.lastIncludeTerm = args.LastIncludedTerm
	reply.Success = true
	reply.LastIncludedIndex = rf.lastIncludeIdx
	reply.LastIncludedTerm = rf.lastIncludeTerm
	if rf.Last_Applied_Idx < rf.lastIncludeIdx {
		rf.Last_Applied_Idx = rf.lastIncludeIdx
	}
	if rf.Committed_Idx < rf.lastIncludeIdx {
		rf.Committed_Idx = rf.lastIncludeIdx
	}
	rf.persist()
	rf.persister.Save(rf.persister.ReadRaftState(), args.Data)
	applyMsg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	rf.mu.Unlock()
	rf.snap_lock.Lock()
	rf.snapshot_idx = applyMsg.SnapshotIndex
	rf.ApplyChan <- applyMsg
	rf.snap_lock.Unlock()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	DPrintf("Node{%v} snapshot index{%v} snapshot{%v} 1", rf.me, index, snapshot)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("Node{%v} snapshot index{%v} snapshot{%v}", rf.me, index, snapshot)
	if rf.lastIncludeIdx >= index {
		DPrintf("snapshot invaild index{%v} preindex{%v} term{%v}", index, rf.lastIncludeIdx, rf.CurrentTerm)
		return
	}
	snapshot_array := make([]LogEntry, 1)
	snapshot_array[0].Log_Term = rf.Log_Array[index-rf.lastIncludeIdx].Log_Term
	snapshot_array = append(snapshot_array, rf.Log_Array[index+1-rf.lastIncludeIdx:]...)
	rf.Log_Array = snapshot_array
	rf.lastIncludeIdx = index
	rf.lastIncludeTerm = snapshot_array[0].Log_Term
	DPrintf("node{%v} snapshot{%v}", rf.me, snapshot)
	rf.persist()
	rf.persister.Save(rf.persister.ReadRaftState(), snapshot)

}
func (rf *Raft) CondInstallSnapshot(lastIncludeIdx int, lastIncludeTerm int, snapshot []byte) bool {
	DPrintf("Node{%d} CondSnapshot", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.Committed_Idx > lastIncludeIdx {
		return false
	}
	log := rf.Log_Array[0:1]
	log[0].Log_Term = lastIncludeTerm
	if rf.GetLastLog().Index > lastIncludeIdx {
		log = append(log, rf.Log_Array[lastIncludeIdx+1-rf.lastIncludeIdx:]...)
	}
	rf.Log_Array = log
	rf.lastIncludeIdx = lastIncludeIdx
	rf.lastIncludeTerm = lastIncludeTerm
	rf.Last_Applied_Idx = lastIncludeIdx
	rf.Committed_Idx = lastIncludeIdx
	rf.persist()
	rf.persister.Save(rf.persister.ReadRaftState(), snapshot)
	return true
}

func (rf *Raft) ShouldSnap(commandIdx int, SnapIndex int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return commandIdx <= rf.persister.RaftStateSize() && commandIdx != -1
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Candidate_Curr_Term int
	Candidate_Id        int
	Last_Log_Index      int
	Last_Log_Term       int
	Seq                 uint64
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Current_Term int
	Vote_Granted bool
	Seq          uint64
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	DPrintf("Rpc requestVote start %v", rf.me)
	rf.mu.Lock()
	reply.Seq = args.Seq
	DPrintf("Rpc get lock %v", rf.me)
	defer rf.mu.Unlock()
	reply.Vote_Granted = false
	if rf.CurrentTerm > args.Candidate_Curr_Term || (rf.CurrentTerm == args.Candidate_Curr_Term && rf.VoteFor != -1 && rf.VoteFor != args.Candidate_Id) {
		reply.Current_Term = rf.CurrentTerm
		reply.Vote_Granted = false
		// DPrintf(args.Candidate_Id, " vote grand fail", rf.me)
		return
	}
	if args.Candidate_Curr_Term > rf.CurrentTerm {
		DPrintf("request is higer!!!!")
		rf.ToFollower()
		rf.VoteFor = -1
		rf.CurrentTerm = args.Candidate_Curr_Term
		rf.persist()
		// // my extra add
		// reply.Vote_Granted = false
		// reply.Current_Term = args.Candidate_Curr_Term
		// return
		// //end
	}
	// election limitation
	DPrintf("Node{%v} argslast{%v}, index{%v} rf.lastterm{%v}, rf.lastindex{%v}", rf.me, args.Last_Log_Term, args.Last_Log_Index, rf.GetLastLog().Log_Term, rf.GetLastLog().Index)
	if args.Last_Log_Term < rf.GetLastLog().Log_Term || args.Last_Log_Term < rf.lastIncludeTerm {
		reply.Current_Term = rf.CurrentTerm
		reply.Vote_Granted = false
		DPrintf("Node{%v} vote grand fail the log term not new, Node{%v}", args.Candidate_Id, rf.me)
		return
	}
	if args.Last_Log_Term == rf.GetLastLog().Log_Term && (args.Last_Log_Index < rf.GetLastLog().Index || args.Last_Log_Index < rf.lastIncludeIdx) {
		reply.Current_Term = rf.CurrentTerm
		reply.Vote_Granted = false
		DPrintf("Node{%v} vote grand fail the length is small, Node{%v}", args.Candidate_Id, rf.me)
		return
	}
	// 两个条件均满足投true

	rf.VoteFor = args.Candidate_Id
	reply.Current_Term = rf.CurrentTerm
	rf.persist()
	DPrintf("Node{%v} vote grand , Node{%v}", args.Candidate_Id, rf.me)
	rf.ResetElection()
	reply.Vote_Granted = true
	// TODO: term is used for the candidate to update itself

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
	// args.Candidate_Curr_Term = rf.CurrentTerm
	// args.Candidate_Id = rf.me
	// TODO: Add the last_log_term and idx
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
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
func (rf *Raft) AppendNewEntries(command interface{}) LogEntry {
	new_entries := LogEntry{Command: command, Log_Term: rf.CurrentTerm, Index: rf.lastIncludeIdx + len(rf.Log_Array)}
	rf.Log_Array = append(rf.Log_Array, new_entries)
	rf.persist()
	for i := range rf.peers {
		// rf.Match_Idx[i] = rf.Committed_Idx
		rf.Next_Idx[i] = rf.GetLastLog().Index
	}
	rf.Match_Idx[rf.me] = rf.GetLastLog().Index
	return new_entries

}
func (rf *Raft) produceSnapshotArgs() SnapArgs {
	rf.persister.mu.Lock()
	defer rf.persister.mu.Unlock()
	return SnapArgs{
		Leader_Term:       rf.CurrentTerm,
		Leader_Id:         rf.me,
		LastIncludedIndex: rf.lastIncludeIdx,
		LastIncludedTerm:  rf.lastIncludeTerm,
		Data:              rf.persister.snapshot,
	}
}
func (rf *Raft) processSnapshotRepy(peer int, request *SnapArgs, reply *SnapReply) {
	if rf.Raft_Status != Leader || request.Leader_Term != rf.CurrentTerm {
		DPrintf("Leader Node{%v} received {%v} Snapshot stale!!", rf.me, rf.dead)
		return
	}
	DPrintf("Leader Node{%v} received {%v} Snapshot", rf.me, peer)
	if rf.CurrentTerm < reply.Term {
		rf.CurrentTerm, rf.VoteFor = reply.Term, -1
		rf.persist()
		rf.ToFollower()
	}
	if reply.Success {
		// may be bug
		DPrintf("Leader Node{%v} received {%v} Snapshot index{%v} success!", rf.me, peer, rf.lastIncludeIdx)
		rf.Next_Idx[peer] = reply.LastIncludedIndex + 1
		rf.Match_Idx[peer] = reply.LastIncludedTerm
	} else {
		// my add bug
		rf.Next_Idx[peer] = reply.LastIncludedIndex + 1
	}
}

func (rf *Raft) replicateOneRound(peer int) {
	rf.mu.Lock()
	if rf.Raft_Status != Leader {
		DPrintf("Node{%v} isn't leader", rf.me)
		rf.mu.Unlock()
		return
	}
	prevLogIndex := rf.Next_Idx[peer] - 1
	if prevLogIndex < 0 {
		DPrintf("Node{%v}'s to peer{%v} prevLogIndex{%v} to 0", rf.me, peer, prevLogIndex)
		prevLogIndex = 0
	}
	if prevLogIndex < rf.lastIncludeIdx {
		DPrintf("Leader Node{%v} to peer{%v} jump into snapshot rpc prevLogIndex{%v} lastidx{%v}", rf.me, peer, prevLogIndex, rf.lastIncludeIdx)
		request := rf.produceSnapshotArgs()
		rf.mu.Unlock()
		reply := new(SnapReply)
		if rf.sendSnapshot(peer, &request, reply) {
			rf.mu.Lock()
			rf.processSnapshotRepy(peer, &request, reply)
			rf.mu.Unlock()
		}
	} else {
		request := rf.produceAppendRequest(prevLogIndex)
		rf.mu.Unlock()

		reply := AppendReply{}
		if rf.SendAppendEntries(peer, &request, &reply) {
			rf.mu.Lock()
			DPrintf("Node{%v} from peer{%v} term{%v}pos here but lock content", rf.me, peer, rf.CurrentTerm)
			rf.processAppendReply(peer, request, reply)
			rf.mu.Unlock()
		} else {
			DPrintf("Leader Node{%v} to Node {%v} append rpc failed ", rf.me, peer)
		}
	}
}

func (rf *Raft) processAppendReply(peer int, args AppendArgs, reply AppendReply) {
	DPrintf("Node{%v} tp peer {%v} next_idx{%v %v}", rf.me, peer, rf.Next_Idx[peer]-1, args.PrevLogIndex)
	reply.PrevLogIndex = int(math.Min(float64(reply.PrevLogIndex), float64(rf.Next_Idx[peer])))
	if args.Seq != reply.Seq || rf.Raft_Status != Leader || rf.CurrentTerm != args.Leader_Term || reply.PrevLogIndex < rf.lastIncludeIdx {
		DPrintf("Leader Node{%v} from Node{%v}rpc order delay", rf.me, peer)
		DPrintf("Node{%v} pre_idx{%v} lastIncludeIdx{%v}", rf.me, reply.PrevLogIndex, rf.lastIncludeIdx)
		DPrintf("{%v %v} {%v %v} {%v %v} {%v %v}", reply.PrevLogIndex, rf.Next_Idx[peer]-1, args.Seq, reply.Seq, rf.Raft_Status, Leader, rf.CurrentTerm, args.Leader_Term)
		return
	}
	//发现更大term的candidate, 转变为follwer
	if !reply.Success && reply.Term > args.Leader_Term {
		DPrintf("find the leadr and change state to follower")
		rf.ToFollower()
		rf.CurrentTerm, rf.VoteFor = reply.Term, -1
		rf.persist()
		return
	}
	if !reply.Success && reply.Term < args.Leader_Term {
		log.Fatalf("reply term smaller")
	}
	if !reply.Success && reply.Term == args.Leader_Term {
		//日志不一致失败，减少next_id重试
		DPrintf("Node{%v}'s to peer{%v} pre_idx become{%v}", rf.me, peer, rf.Next_Idx[peer]-1)
		if reply.LastIdx == 0 {
			rf.Next_Idx[peer] = rf.GetIdxPreTermLogEntry(reply.PrevLogIndex)
			if rf.Next_Idx[peer] < rf.Match_Idx[peer]+1 {
				rf.Next_Idx[peer] = rf.Match_Idx[peer] + 1
			}
			// DPrintf("leader node{%v} log{%v}", rf.me, rf.Log_Array) to do
		} else {
			rf.Next_Idx[peer] = reply.LastIdx + 1
			DPrintf("Node{%v} to peer{%v} conflict and to the last_idx{%v} + 1", rf.me, peer, reply.LastIdx)
		}
		// rf.Next_Idx[peer] = int(math.Max(float64(rf.Next_Idx[peer]), float64(rf.lastIncludeIdx+1)))
		DPrintf("Node{%v}'s to peer{%v} next_idx become{%v}", rf.me, peer, rf.Next_Idx[peer])
		return
	}
	if reply.Success {
		DPrintf("Leader Node{%v} receive the Node{%v} append success next_id{%v} ", rf.me, peer, rf.Next_Idx[peer])
		// update next_id and math_id
		// 防止两个rpc同时到达乱序
		rf.Next_Idx[peer] = reply.PrevLogIndex + 1 + len(args.Entries)
		DPrintf("Node{%v} next id change to {%v}  by adding{%v}", rf.me, rf.Next_Idx[peer], len(args.Entries))
		rf.Match_Idx[peer] = int(math.Max(float64(rf.Next_Idx[peer]-1), float64(rf.Match_Idx[peer])))
		//取match_idx的中位数来做commit_idx,因为满足一半peers已经commit了
		DPrintf("Node{%v} match_array{%v}}", rf.me, rf.Match_Idx)
		matchIdx := make([]int, 0)
		for i := 0; i < len(rf.peers); i++ {
			if rf.me != i {
				matchIdx = append(matchIdx, rf.Match_Idx[i])
			}
		}
		matchIdx = append(matchIdx, rf.GetLastLog().Index)
		sort.Ints(matchIdx)
		commit_idx := matchIdx[(len(matchIdx))/2]
		DPrintf("Node{%v} match_array{%v} and commit_idx{%v}", rf.me, rf.Match_Idx, commit_idx)
		if commit_idx > rf.Committed_Idx && rf.Log_Array[commit_idx-rf.lastIncludeIdx].Log_Term == rf.CurrentTerm {
			DPrintf("Leader Node{%v} commit increase from{%v} to {%v} and signal", rf.me, rf.Committed_Idx, commit_idx)
			rf.Committed_Idx = commit_idx
			//通知applier协程
			rf.ApplyCond.Signal()
		}
	} else {
		rf.ToFollower()
		DPrintf("Node{%v} to follower may be heartbeat time out to election", rf.me)
	}
}
func (rf *Raft) produceAppendRequest(prev_log_idx int) AppendArgs {
	DPrintf("Node{%v} len log{%v}, prev_log_idx{%v}", rf.me, len(rf.Log_Array), prev_log_idx)
	rf.seq++
	return AppendArgs{
		Leader_Term:   rf.CurrentTerm,
		Leader_Id:     rf.me,
		PrevLogIndex:  prev_log_idx,
		PrevLogTerm:   rf.Log_Array[prev_log_idx-rf.lastIncludeIdx].Log_Term,
		Entries:       rf.Log_Array[prev_log_idx+1-rf.lastIncludeIdx:],
		Leader_Commit: rf.Committed_Idx,
		Seq:           rf.seq,
	}
}
func (rf *Raft) replicator(peer int) {
	rf.ReplicatorCond[peer].L.Lock()
	defer rf.ReplicatorCond[peer].L.Unlock()
	for !rf.killed() {
		// if there is no need to replicate entries for this peer, just release CPU and wait other goroutine's signal if service adds new Command
		// if this peer needs replicating entries, this goroutine will call replicateOneRound(peer) multiple times until this peer catches up, and then wait
		for !rf.NeedReplicating(peer) {
			rf.ReplicatorCond[peer].Wait()
		}
		rf.replicateOneRound(peer)
	}
}
func (rf *Raft) GetLastLog() LogEntry {
	return rf.Log_Array[len(rf.Log_Array)-1]
}
func (rf *Raft) GetFirstLog() LogEntry {
	// to do
	return rf.Log_Array[1]
}
func (rf *Raft) NeedReplicating(peer int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.Raft_Status == Leader && rf.Match_Idx[peer] < rf.GetLastLog().Index
}

func (rf *Raft) BroadcastHeartbeat(isHeartBeat bool) {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		if isHeartBeat {
			DPrintf("Node{%v} send heartbeat to the Node{%v}", rf.me, peer)
			go rf.replicateOneRound(peer)
		} else {
			DPrintf("Node{%v} start to replicate to the Node{%v}", rf.me, peer)
			rf.ReplicatorCond[peer].Signal()
		}
	}
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.Raft_Status != Leader {
		return -1, -1, false
	}
	new_entries := rf.AppendNewEntries(command)
	DPrintf("...............................................................................")
	log.Printf("{Node %v} receive a new commodidx[%v] to replicate in term %v", rf.me, new_entries, rf.CurrentTerm)
	DPrintf("...............................................................................")
	rf.BroadcastHeartbeat(false)
	rf.HeartReset()
	index := new_entries.Index
	term := rf.CurrentTerm
	isLeader := true
	// Your code here (2B).

	return index, term, isLeader
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
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ResetElection() {
	rf.ElectionTimeOut = time.Now()
	rf.ElectTime = 150 + rand.Int()%150

}
func (rf *Raft) ElectionIfOut() bool {
	if rf.Raft_Status == Leader {
		return false
	}
	// log.Println(rf.me, " ", time.Now(), rf.ElectionTimeOut.Add(time.Millisecond*time.Duration(rf.ElectTime)))
	return time.Now().After(rf.ElectionTimeOut.Add(time.Millisecond * time.Duration(rf.ElectTime)))
}

func (rf *Raft) GetIdxPreTermLogEntry(pre_idx int) int {
	pre_term := rf.Log_Array[pre_idx-rf.lastIncludeIdx].Log_Term
	for i := pre_idx - 1 - rf.lastIncludeIdx; i >= 0; i-- {
		if rf.Log_Array[i].Log_Term != pre_term {
			return rf.Log_Array[i].Index + 1
		}
	}
	return 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.mu.Lock()
		// log.Println(time.Now())
		if rf.ElectionIfOut() {
			// 开始新的选举
			DPrintf("Node{%v} state{%v}candidate start and stry to get votes", rf.me, rf.Raft_Status)
			rf.CandidateAction()
			// 重设置计时器
			rf.ResetElection()
		}
		rf.mu.Unlock()
		// Your code here (2A)
		// Check if a leader election should be started.
		// TODO:

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}
func (rf *Raft) HeartReset() {
	rf.HeartBeatTimeOut = time.Now()
}
func (rf *Raft) HeartIfOut() bool {
	return time.Now().After(rf.HeartBeatTimeOut.Add(time.Duration(rf.HeartTime) * time.Millisecond))
}
func (rf *Raft) Heart() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.HeartIfOut() {
			if rf.Raft_Status == Leader {
				DPrintf("Node{%v} start heart beat", rf.me)
				// 开始心跳监测
				rf.BroadcastHeartbeat(true)
				rf.HeartReset()
			}
		}
		rf.mu.Unlock()
		// Your code here (2A)
		// Check if a leader election should be started.
		// TODO:

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 30 + (rand.Int63() % 5)
		time.Sleep(time.Duration(ms) * time.Millisecond)
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
	DPrintf("the new raft is {%v}", me)
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.Log_Array = make([]LogEntry, 1)
	// Your initialization code here (2A, 2B, 2C).
	rf.VoteFor = -1
	rf.CurrentTerm = 0
	rf.Raft_Status = Follower
	rf.Committed_Idx = 0
	rf.Last_Applied_Idx = 0
	rf.HeartTime = 50
	rf.HeartBeatTimeOut = time.Now()
	rf.ElectTime = 150 + rand.Int()%150
	rf.ElectionTimeOut = time.Now()
	rf.Next_Idx = make([]int, len(rf.peers))
	rf.Match_Idx = make([]int, len(rf.peers))
	rf.ApplyChan = applyCh
	// rf.ApplyChanTemp = make(chan ApplyMsg, 2000)
	rf.ReplicatorCond = make([]*sync.Cond, len(rf.peers))
	rf.ApplyCond = *sync.NewCond(&rf.mu)
	for peer := range rf.peers {
		rf.Next_Idx[peer] = 1
		rf.Match_Idx[peer] = 0
		if peer != rf.me {
			rf.ReplicatorCond[peer] = sync.NewCond(&sync.Mutex{})
			go rf.replicator(peer)
		}
	}
	// ..........to change the idx start from 1
	// rf.Match_Idx
	// TODO:
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	for peers := range rf.peers {
		rf.Next_Idx[peers] = rf.GetLastLog().Index + 1
	}
	rf.Log_Array[0].Log_Term = rf.lastIncludeTerm
	rf.Committed_Idx = rf.lastIncludeIdx
	rf.Last_Applied_Idx = rf.lastIncludeIdx
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.Heart()
	// go rf.applier()
	go rf.Applier()
	return rf
}

// func (rf *Raft) applier() {
// 	for msg := range rf.ApplyChanTemp {
// 		rf.ApplyChan <- msg
// 	}
// }

func (rf *Raft) Applier() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.Last_Applied_Idx >= rf.Committed_Idx || rf.GetLastLog().Index+1 <= rf.Committed_Idx {
			DPrintf("here pos the")
			rf.ApplyCond.Wait()
			DPrintf("Node{%v}, last_applied{%v}, commited_idx{%v}", rf.me, rf.Last_Applied_Idx, rf.Committed_Idx)
			rf.Committed_Idx = int(math.Min(float64(rf.Committed_Idx), float64(rf.GetLastLog().Index)))
			DPrintf("Node{%v} {log len{%d}, commit_id{%d}}", rf.me, len(rf.Log_Array), rf.Committed_Idx)
		}
		// DPrintf("Node{%d}commit_idx{%d} last_applied_idx{%d} log{%v}, logtoapply{%v}", rf.me, rf.Committed_Idx, rf.Last_Applied_Idx, rf.Log_Array, rf.Log_Array[rf.Last_Applied_Idx+1:rf.Committed_Idx+1])
		entries := make([]LogEntry, rf.Committed_Idx-rf.Last_Applied_Idx)
		copy(entries, rf.Log_Array[rf.Last_Applied_Idx+1-rf.lastIncludeIdx:rf.Committed_Idx+1-rf.lastIncludeIdx])
		// DPrintf("Node{%v} enries{%v}", rf.me, entries)
		commit_idx := rf.Committed_Idx
		rf.mu.Unlock()
		DPrintf("Node{%v}pos here1", rf.me)
		rf.snap_lock.Lock()
		if rf.snapshot_idx >= commit_idx {
			rf.snap_lock.Unlock()
			continue
		}
		for _, entry := range entries {
			rf.ApplyChan <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
				CommandTerm:  entry.Log_Term,
			}
		}
		rf.snap_lock.Unlock()
		DPrintf("Node{%v} pos here2", rf.me)
		rf.mu.Lock()
		DPrintf("{Node %v} applies entries %v-%v in term %v", rf.me, rf.Last_Applied_Idx, rf.Committed_Idx, rf.CurrentTerm)
		rf.Last_Applied_Idx = int(math.Max(float64(rf.Last_Applied_Idx), float64(commit_idx)))
		rf.mu.Unlock()
	}
}

// heartbeat implement
type AppendArgs struct {
	Leader_Term   int
	Leader_Id     int
	PrevLogIndex  int
	PrevLogTerm   int
	Entries       []LogEntry
	Leader_Commit int
	Seq           uint64
}
type AppendReply struct {
	Term         int // update leader itself
	Success      bool
	PrevLogIndex int // 防止rpc乱序到达
	Seq          uint64
	LastIdx      int
}

func (rf *Raft) SendAppendEntries(server int, args *AppendArgs, reply *AppendReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) CandidateAction() {
	DPrintf("start candidate")
	rf.Raft_Status = Candidate
	rf.CurrentTerm++
	rf.persist()
	DPrintf("Node{%v} currTerm{%v}", rf.me, rf.CurrentTerm)
	rf.VoteFor = rf.me
	rf.persist()
	DPrintf("candidate start get vote and vote itself")
	vote_cnt := 1
	var term int
	if len(rf.Log_Array) == 0 {
		term = rf.lastIncludeTerm
	} else {
		term = rf.GetLastLog().Log_Term
	}
	rf.seq++
	args := RequestVoteArgs{
		Candidate_Curr_Term: rf.CurrentTerm,
		Candidate_Id:        rf.me,
		Last_Log_Index:      int(math.Max(float64(rf.GetLastLog().Index), float64(rf.lastIncludeIdx))),
		Last_Log_Term:       term,
		Seq:                 rf.seq,
	}
	for i := range rf.peers {
		if i != rf.me {
			//last_idx := len(rf.Log_Array) - 1
			go func(idx int) {
				rf.mu.Lock()
				if rf.Raft_Status != Candidate {
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()
				reply := RequestVoteReply{}
				if rf.sendRequestVote(idx, &args, &reply) {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					// to do
					if rf.CurrentTerm == args.Candidate_Curr_Term && rf.Raft_Status == Candidate && args.Seq == reply.Seq {
						DPrintf("pos here")
						if reply.Vote_Granted {
							vote_cnt++
							DPrintf("Node{%v} get vote from Node{%v} in term{%v} and cur_get_cnt{%v}", rf.me, idx, rf.CurrentTerm, vote_cnt)
							if vote_cnt > len(rf.peers)/2 {
								rf.ToLeader()
								rf.BroadcastHeartbeat(true)
								rf.HeartReset()
								DPrintf("Node {%v} heart sync finish, change to leader", rf.me)
							}
						} else if reply.Current_Term > rf.CurrentTerm {
							DPrintf("the leader{%v} to follower ", rf.me)
							rf.ToFollower()
							rf.CurrentTerm, rf.VoteFor = reply.Current_Term, -1
							rf.persist()
						}
					}
				} else {
					DPrintf("rpc fail vote")

				}
				// wg.Done()
			}(i)
			// 并行发送到其他所以机器上投票

		}
	}
}
func (rf *Raft) ToLeader() {
	DPrintf("Node{%v} to Leader", rf.me)
	rf.Raft_Status = Leader
	for peer := range rf.peers {
		rf.Next_Idx[peer] = rf.GetLastLog().Index + 1
		if peer == rf.me {
			rf.Match_Idx[peer] = rf.Next_Idx[peer] - 1
		} else {
			rf.Match_Idx[peer] = 0
		}
	}
	// rf.Committed_Idx = rf.lastIncludeIdx
}
func (rf *Raft) ToFollower() {
	DPrintf("Node{%v} to follower", rf.me)
	rf.Raft_Status = Follower
}
func (rf *Raft) checkMyLog(prev_log_idx int, args []LogEntry) bool {
	Log := rf.Log_Array[prev_log_idx+1-rf.lastIncludeIdx:]
	if len(Log) == 0 {
		return false
	}
	flag := true
	if len(Log) > len(args) {
		for i := 0; i < len(args); i++ {
			if Log[i].Index != args[i].Index || Log[i].Log_Term != args[i].Log_Term {
				flag = false
				break
			}
		}
	} else {
		flag = false
	}
	return flag
}
func (rf *Raft) AppendEntries(args *AppendArgs, reply *AppendReply) {
	rf.mu.Lock()
	reply.PrevLogIndex = args.PrevLogIndex
	reply.Seq = args.Seq
	DPrintf("%v append %v", args.Leader_Id, rf.me)
	defer rf.mu.Unlock()
	if args.Leader_Term < rf.CurrentTerm {
		DPrintf("Node{%v} receive the log smaller", rf.me)
		reply.Term = rf.CurrentTerm
		reply.Success = false
		return
	}
	if args.Leader_Term > rf.CurrentTerm {
		rf.CurrentTerm, rf.VoteFor = args.Leader_Term, -1
		rf.persist()
	}
	// log.Println("append success", rf.me, "  ", time.Now())
	rf.ToFollower()
	rf.ResetElection()
	// DPrintf("Node{%v} commit_idx{%v} apply{%v}, prevlog{%v}, prevterm{%v} last{%v} log{%v} append{%v}", rf.me, rf.Committed_Idx, rf.Last_Applied_Idx, args.PrevLogIndex, args.PrevLogTerm, rf.lastIncludeIdx, rf.Log_Array, args.Entries) to do
	//日志不match,删除prevlogIndex及其以后的log, 并返回false
	if args.PrevLogIndex != 0 {
		if (args.PrevLogIndex > rf.GetLastLog().Index && args.PrevLogIndex > rf.lastIncludeIdx) || args.PrevLogIndex < rf.lastIncludeIdx || rf.Log_Array[args.PrevLogIndex-rf.lastIncludeIdx].Log_Term != args.PrevLogTerm {
			reply.Success = false
			reply.Term = args.Leader_Term
			if args.PrevLogIndex < rf.lastIncludeIdx {
				reply.LastIdx = rf.lastIncludeIdx
				DPrintf("the append rpc Node{%v} conflict the last_idx{%v} and prevLogIndex{%v}", rf.me, rf.lastIncludeIdx, args.PrevLogIndex)
				return
			}
			DPrintf("the follower{%d} cut log to {%v} ", rf.me, args.PrevLogIndex-1)
			if args.PrevLogIndex <= rf.GetLastLog().Index {
				rf.Log_Array = rf.Log_Array[:args.PrevLogIndex-rf.lastIncludeIdx]
				rf.persist()
			}
			return
		} else if args.PrevLogIndex < rf.lastIncludeIdx {
			reply.Success = false
			reply.Term = args.Leader_Term

			reply.LastIdx = rf.lastIncludeIdx
			DPrintf("the append rpc Node{%v} conflict the last_idx{%v} and prevLogIndex{%v}", rf.me, rf.lastIncludeIdx, args.PrevLogIndex)
			return
		}
	} else {
		reply.Success = false
		reply.Term = args.Leader_Term
		if args.PrevLogIndex < rf.lastIncludeIdx {
			reply.LastIdx = rf.lastIncludeIdx
			DPrintf("the append rpc Node{%v} conflict the last_idx{%v} and prevLogIndex{%v}", rf.me, rf.lastIncludeIdx, args.PrevLogIndex)
			return
		}
		DPrintf("prevlog_idx = 0")
	}
	// 从PrevLogIndex + 1也就是next_id开始追加存储
	// 检查是否需要截断添加，防止rpc乱序到达，截断
	if !rf.checkMyLog(args.PrevLogIndex, args.Entries) {
		rf.Log_Array = rf.Log_Array[:args.PrevLogIndex-rf.lastIncludeIdx+1]
		rf.Log_Array = append(rf.Log_Array, args.Entries...)
		rf.persist()
	} else {
		DPrintf("the log before will cut the log, and branch here")
	}
	// DPrintf("Node pos2 {%v} commit_idx{%v} apply{%v}, prevlog{%v}, prevterm{%v} last{%v} log{%v} append{%v}", rf.me, rf.Committed_Idx, rf.Last_Applied_Idx, args.PrevLogIndex, args.PrevLogTerm, rf.lastIncludeIdx, rf.Log_Array, args.Entries)
	// 设置本地commit为最新日志和leader_commit中较小的一个
	rf.CurrentTerm = args.Leader_Term
	if rf.Committed_Idx < args.Leader_Commit {
		rf.Committed_Idx = int(math.Min(float64(args.Leader_Commit), float64(rf.GetLastLog().Index)))
		rf.ApplyCond.Signal()
		DPrintf("Node{%v} commid{%v} change and notify", rf.me, rf.Committed_Idx)
	}
	//心跳内容
	reply.Success = true
	rf.persist()
	//TODO:2,3,4,5 in the paper

}
