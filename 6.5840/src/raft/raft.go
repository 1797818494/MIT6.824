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

	"log"
	"math"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
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
	Raft_Status      Status
	CurrentTerm      int
	VoteFor          int
	Log_Array        []LogEntry
	Committed_Idx    int
	ApplyChan        chan ApplyMsg
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
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
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
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Candidate_Curr_Term int
	Candidate_Id        int
	Last_Log_Index      int
	Last_Log_Term       int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Current_Term int
	Vote_Granted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	log.Println("Rpc requestVote start ", rf.me)
	rf.mu.Lock()
	log.Println("Rpc get lock", rf.me)
	defer rf.mu.Unlock()
	reply.Vote_Granted = false
	if rf.CurrentTerm > args.Candidate_Curr_Term || (rf.CurrentTerm == args.Candidate_Curr_Term && rf.VoteFor != -1 && rf.VoteFor != args.Candidate_Id) {
		reply.Current_Term = rf.CurrentTerm
		reply.Vote_Granted = false
		log.Println(args.Candidate_Id, " vote grand fail", rf.me)
		return
	}
	if args.Candidate_Curr_Term > rf.CurrentTerm {
		log.Println("request is higer!!!!")
		rf.ToFollower()
		rf.VoteFor = -1
		rf.CurrentTerm = args.Candidate_Curr_Term
	}
	// election limitation
	if args.Last_Log_Term < rf.Log_Array[len(rf.Log_Array)-1].Log_Term {
		reply.Current_Term = rf.CurrentTerm
		reply.Vote_Granted = false
		log.Println(args.Candidate_Id, " vote grand fail the log term not new", rf.me)
		return
	}
	if args.Last_Log_Term == rf.Log_Array[len(rf.Log_Array)-1].Log_Term && args.Last_Log_Index < len(rf.Log_Array)-1 {
		reply.Current_Term = rf.CurrentTerm
		reply.Vote_Granted = false
		log.Println(args.Candidate_Id, " vote grand fail the log length is small", rf.me)
		return
	}
	// 两个条件均满足投true

	rf.VoteFor = args.Candidate_Id
	reply.Current_Term = rf.CurrentTerm
	log.Println(args.Candidate_Id, " vote grand ", rf.me)
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
	new_entries := LogEntry{Command: command, Log_Term: rf.CurrentTerm, Index: len(rf.Log_Array)}
	rf.Log_Array = append(rf.Log_Array, new_entries)
	rf.Log_Array[len(rf.Log_Array)-1].Index = len(rf.Log_Array) - 1
	for i := range rf.peers {
		rf.Match_Idx[i] = rf.Committed_Idx
		rf.Next_Idx[i] = len(rf.Log_Array) - 1
	}
	rf.Match_Idx[rf.me] = len(rf.Log_Array) - 1
	return new_entries

}
func (rf *Raft) GetFirstLog() LogEntry {
	// to do
	return rf.Log_Array[0]
}
func (rf *Raft) replicateOneRound(peer int) {
	rf.mu.Lock()
	if rf.Raft_Status != Leader {
		rf.mu.Unlock()
		return
	}
	prevLogIndex := rf.Next_Idx[peer] - 1
	if prevLogIndex < 0 {
		DPrintf("Node{%v}'s to peer{%v} prevLogIndex{%v} to 0", rf.me, peer, prevLogIndex)
		prevLogIndex = 0
	}

	if prevLogIndex == -1 {
		log.Fatal("the unpossible branch")
	} else {
		request := rf.produceAppendRequest(prevLogIndex)
		rf.mu.Unlock()

		reply := AppendReply{}
		if rf.SendAppendEntries(peer, &request, &reply) {
			rf.mu.Lock()
			rf.processAppendReply(peer, request, reply)
			rf.mu.Unlock()
		} else {
			DPrintf("Leader Node{%v} to Node {%v} append rpc failed ", rf.me, peer)
		}
	}
}

func (rf *Raft) processAppendReply(peer int, args AppendArgs, reply AppendReply) {
	if reply.PrevLogIndex != rf.Next_Idx[peer]-1 {
		log.Printf("rpc order delay")
		return
	}
	if !reply.Success && reply.Term == args.Leader_Term {
		//日志不一致失败，减少next_id重试
		rf.Next_Idx[peer] = rf.GetIdxPreTerm(rf.Next_Idx[peer] - 1)
		log.Printf("Node{%v}'s next_idx become{%v}", rf.me, rf.Next_Idx[peer])
		return
	}
	//发现更大term的candidate, 转变为follwer
	if !reply.Success && reply.Term > args.Leader_Term {
		log.Println("find the leadr and change state to follower")
		rf.ToFollower()
		rf.CurrentTerm, rf.VoteFor = reply.Term, -1
		return
	}
	if !reply.Success && reply.Term < args.Leader_Term {
		log.Fatalf("reply term smaller")
	}
	if reply.Success {
		log.Printf("Leader Node{%v} receive the Node{%v} append success next_id{%v} log_len{%v}, add{%v}", rf.me, peer, rf.Next_Idx[peer], len(rf.Log_Array), len(args.Entries))
		// update next_id and math_id
		// 防止两个rpc同时到达乱序
		rf.Next_Idx[peer] += len(args.Entries)
		rf.Match_Idx[peer] = rf.Next_Idx[peer] - 1
		//取match_idx的中位数来做commit_idx,因为满足一半peers已经commit了
		DPrintf("match_array{%v}}", rf.Match_Idx)
		matchIdx := make([]int, 0)
		for i := 0; i < len(rf.peers); i++ {
			if rf.me != i {
				matchIdx = append(matchIdx, rf.Match_Idx[i])
			}
		}
		matchIdx = append(matchIdx, len(rf.Log_Array)-1)
		sort.Ints(matchIdx)
		commit_idx := matchIdx[(len(matchIdx))/2]
		DPrintf("match_array{%v} and commit_idx{%v}", rf.Match_Idx, commit_idx)
		if commit_idx > rf.Committed_Idx {
			DPrintf("Leader Node{%v} commit increase from{%v} to {%v} and signal", rf.me, rf.Committed_Idx, commit_idx)
			rf.Committed_Idx = commit_idx
			//通知applier协程
			rf.ApplyCond.Signal()
		}
	}
}
func (rf *Raft) produceAppendRequest(prev_log_idx int) AppendArgs {
	DPrintf("len log{%v}, prev_log_idx{%v}", len(rf.Log_Array), prev_log_idx)
	return AppendArgs{
		Leader_Term:   rf.CurrentTerm,
		Leader_Id:     rf.me,
		PrevLogIndex:  prev_log_idx,
		PrevLogTerm:   rf.Log_Array[prev_log_idx].Log_Term,
		Entries:       rf.Log_Array[prev_log_idx+1:],
		Leader_Commit: rf.Committed_Idx,
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
	log.Printf("...............................................................................")
	DPrintf("{Node %v} receive a new commod[%v] to replicate in term %v", rf.me, new_entries, rf.CurrentTerm)
	DPrintf("...............................................................................")
	rf.BroadcastHeartbeat(false)
	rf.HeartReset()
	index := new_entries.Index
	term := rf.CurrentTerm
	isLeader := true
	// for i := range rf.peers {
	// 	if i != rf.me {
	// 		rf.Next_Idx[i] = len(rf.Log_Array) - 1
	// 	}
	// }
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
	rf.ElectTime = 500 + rand.Int()%500

}
func (rf *Raft) ElectionIfOut() bool {
	if rf.Raft_Status == Leader {
		return false
	}
	log.Println(rf.me, " ...... ", time.Now(), rf.ElectionTimeOut.Add(time.Millisecond*time.Duration(rf.ElectTime)))
	return time.Now().After(rf.ElectionTimeOut.Add(time.Millisecond * time.Duration(rf.ElectTime)))
}

func (rf *Raft) GetIdxPreTerm(pre_idx int) int {
	pre_term := rf.Log_Array[pre_idx].Log_Term
	for i := pre_idx - 1; i >= 0; i-- {
		if rf.Log_Array[i].Log_Term != pre_term {
			return i + 1
		}
	}
	return 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.mu.Lock()
		log.Println(time.Now())
		if rf.ElectionIfOut() {
			// 开始新的选举
			log.Println("candidate start .......", rf.me)
			log.Println("state is ", rf.Raft_Status)
			log.Println(rf.me, " start to try to get votes")
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
				log.Println(rf.me, " start heart beat")
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
	log.Println("the new raft is ", me)
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
	rf.HeartTime = 100
	rf.HeartBeatTimeOut = time.Now()
	rf.ElectTime = 500 + rand.Int()%500
	rf.ElectionTimeOut = time.Now()
	rf.Next_Idx = make([]int, len(rf.peers))
	rf.Match_Idx = make([]int, len(rf.peers))
	rf.ApplyChan = applyCh
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

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.Heart()
	go rf.Applier()
	return rf
}

func (rf *Raft) Applier() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.Last_Applied_Idx >= rf.Committed_Idx || len(rf.Log_Array) <= rf.Committed_Idx {
			rf.ApplyCond.Wait()
			DPrintf("Node{%v}, last_applied{%v}, commited_idx{%v}", rf.me, rf.Last_Applied_Idx, rf.Committed_Idx)
			rf.Committed_Idx = int(math.Min(float64(rf.Committed_Idx), float64(len(rf.Log_Array)-1)))
			DPrintf("{log len{%d}, commit_id{%d}}", len(rf.Log_Array), rf.Committed_Idx)
		}
		DPrintf("Node{%d}commit_idx{%d} last_applied_idx{%d} log{%v}, logtoapply{%v}", rf.me, rf.Committed_Idx, rf.Last_Applied_Idx, rf.Log_Array, rf.Log_Array[rf.Last_Applied_Idx+1:rf.Committed_Idx+1])
		entries := make([]LogEntry, rf.Committed_Idx-rf.Last_Applied_Idx)
		copy(entries, rf.Log_Array[rf.Last_Applied_Idx+1:rf.Committed_Idx+1])
		DPrintf("Node{%v} enries{%v}", rf.me, entries)
		rf.mu.Unlock()
		for _, entry := range entries {
			rf.ApplyChan <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
			}
		}

		rf.mu.Lock()
		DPrintf("{Node %v} applies entries %v-%v in term %v", rf.me, rf.Last_Applied_Idx, rf.Committed_Idx, rf.CurrentTerm)
		rf.Last_Applied_Idx = int(math.Max(float64(rf.Last_Applied_Idx), float64(rf.Committed_Idx)))
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
}
type AppendReply struct {
	Term         int // update leader itself
	Success      bool
	PrevLogIndex int // 防止rpc乱序到达
}

func (rf *Raft) SendAppendEntries(server int, args *AppendArgs, reply *AppendReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) CandidateAction() {
	log.Println("start candidate")
	rf.Raft_Status = Candidate
	rf.CurrentTerm++
	log.Println(rf.CurrentTerm)
	rf.VoteFor = rf.me
	log.Println("candidate start get vote and vote itself")
	vote_cnt := 1
	var term int
	if len(rf.Log_Array) == 0 {
		term = rf.CurrentTerm
	} else {
		term = rf.Log_Array[len(rf.Log_Array)-1].Log_Term
	}
	args := RequestVoteArgs{
		Candidate_Curr_Term: rf.CurrentTerm,
		Candidate_Id:        rf.me,
		Last_Log_Index:      len(rf.Log_Array) - 1,
		Last_Log_Term:       term,
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
					// log.Println(rf.me, "send the rpc to the ", idx)
					// to do
					if rf.CurrentTerm == args.Candidate_Curr_Term && rf.Raft_Status == Candidate {
						log.Println("pos here")
						if reply.Vote_Granted {
							vote_cnt++
							log.Println(rf.me, "get vote numver is", vote_cnt)
							if vote_cnt > len(rf.peers)/2 {
								rf.ToLeader()
								rf.BroadcastHeartbeat(true)
								rf.HeartReset()
								log.Println("heart sync finish, change ", rf.me, " to leader")
							}
						} else if reply.Current_Term >= rf.CurrentTerm {
							log.Println("the leader to follower ", rf.me)
							rf.ToFollower()
							rf.CurrentTerm, rf.VoteFor = reply.Current_Term, -1
						}
					}
				} else {
					log.Println("rpc fail vote")
				}
				// wg.Done()
			}(i)
			// 并行发送到其他所以机器上投票

		}
	}
}
func (rf *Raft) ToLeader() {
	log.Println(rf.me, " to Leader")
	rf.Raft_Status = Leader
}
func (rf *Raft) ToFollower() {
	log.Println(rf.me, " to follower")
	rf.Raft_Status = Follower
}
func (rf *Raft) AppendEntries(args *AppendArgs, reply *AppendReply) {
	rf.mu.Lock()
	reply.PrevLogIndex = args.PrevLogIndex
	log.Println("append ", rf.me)
	defer rf.mu.Unlock()
	if args.Leader_Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.Success = false
		return
	}
	if args.Leader_Term > rf.CurrentTerm {
		rf.CurrentTerm, rf.VoteFor = args.Leader_Term, -1
	}
	rf.ToFollower()
	// log.Println("append success", rf.me, "  ", time.Now())
	rf.ResetElection()

	//日志不match,删除prevlogIndex及其以后的log, 并返回false
	if args.PrevLogIndex != 0 {
		if args.PrevLogIndex >= len(rf.Log_Array) || rf.Log_Array[args.PrevLogIndex].Log_Term != args.PrevLogTerm {
			reply.Success = false
			reply.Term = args.Leader_Term
			// DPrintf("the follower{%d} log from {%v} to {%v} ", rf.me, rf.Log_Array, rf.Log_Array[:args.PrevLogIndex])
			// if args.PrevLogIndex < len(rf.Log_Array) {
			// 	rf.Log_Array = rf.Log_Array[:args.PrevLogIndex]
			// }
			return
		}
	} else {
		DPrintf("prevlog_idx = 0")
	}
	// 从PrevLogIndex + 1也就是next_id开始追加存储
	rf.Log_Array = rf.Log_Array[:args.PrevLogIndex+1]
	rf.Log_Array = append(rf.Log_Array, args.Entries...)
	DPrintf("the follower{%d} log is success log{%v}............", rf.me, rf.Log_Array)
	// 设置本地commit为最新日志和leader_commit中较小的一个
	if rf.Committed_Idx < args.Leader_Commit {
		rf.Committed_Idx = int(math.Min(float64(args.Leader_Commit), float64(len(rf.Log_Array)-1)))
		rf.ApplyCond.Signal()
		DPrintf("Node{%v} commid{%v} change and notify", rf.me, rf.Committed_Idx)
	}
	//心跳内容
	rf.CurrentTerm = args.Leader_Term
	reply.Success = true
	//TODO:2,3,4,5 in the paper

}
