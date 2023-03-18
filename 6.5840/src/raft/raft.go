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
	"math/rand"
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
	Last_Applied_Idx int
	// leader violate and should reinitilize in the start of vote
	Next_Idx         []int
	Match_Idx        []int
	HeartBeatTimeOut time.Time
	ElectionTimeOut  time.Time
	HeartTime        int
	ElectTime        int
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
	// -1 stand for null

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
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
	rf.ElectTime = 500 + rand.Int()%500

}
func (rf *Raft) ElectionIfOut() bool {
	if rf.Raft_Status == Leader {
		return false
	}
	log.Println(rf.me, " ...... ", time.Now(), rf.ElectionTimeOut.Add(time.Millisecond*time.Duration(rf.ElectTime)))
	return time.Now().After(rf.ElectionTimeOut.Add(time.Millisecond * time.Duration(rf.ElectTime)))
}
func (rf *Raft) StartHeart() {
	args := AppendArgs{
		Leader_Term: rf.CurrentTerm,
		Leader_Id:   rf.me,
		// PrevLogTerm:   rf.Log_Array[rf.Next_Idx[idx]].Log_Term,
		// PrevLogIndex:  rf.Next_Idx[idx],
		Entries:       rf.Log_Array,
		Leader_Commit: rf.Committed_Idx,
	}
	for i := range rf.peers {
		if i != rf.me {
			// pre_idx := rf.Next_Idx[i]
			go func(idx int) {
				rf.mu.Lock()
				if rf.Raft_Status != Leader {
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()
				reply := AppendReply{}
				if rf.SendAppendEntries(idx, &args, &reply) {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if !reply.Success {
						log.Println("find the leadr and change state to follower")
						rf.ToFollower()
						rf.CurrentTerm, rf.VoteFor = reply.Term, -1
					}
					// if reply.Success {
					// 	log.Println("append success", idx)
					// 	rf.Next_Idx[idx] = len(rf.Log_Array) - 1
					// 	return
					// }
					// // pre_idx == 0的情况
					// //变化为term中最后一个
					// if pre_idx > 0 && rf.Log_Array[pre_idx-1].Log_Term == rf.Log_Array[pre_idx].Log_Term {
					// 	for pre_idx > 0 && rf.Log_Array[pre_idx-1].Log_Term == rf.Log_Array[pre_idx].Log_Term {
					// 		pre_idx--
					// 	}
					// } else {
					// 	//已经是term最后一个直接，进入下一个
					// 	pre_idx--
					// }
					// rf.Next_Idx[idx] = pre_idx
				} else {
					log.Println("rpc append failed")
				}
			}(i)
		}
	}
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
				rf.StartHeart()
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

	// Your initialization code here (2A, 2B, 2C).
	rf.VoteFor = -1
	rf.CurrentTerm = 1
	rf.Raft_Status = Follower
	rf.Committed_Idx = 0
	rf.Last_Applied_Idx = 0
	rf.HeartTime = 100
	rf.HeartBeatTimeOut = time.Now()
	rf.ElectTime = 500 + rand.Int()%500
	rf.ElectionTimeOut = time.Now()
	// rf.Match_Idx
	// rf.Next_Idx
	// TODO:
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.Heart()

	return rf
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
	Term    int // update leader itself
	Success bool
}

func (rf *Raft) SendAppendEntries(server int, args *AppendArgs, reply *AppendReply) bool {
	// args.Leader_Term = rf.CurrentTerm
	// args.Leader_Id = rf.me
	// TODO:
	// args.PrevLogIndex
	// args.PrevLogTerm
	// args.Leader_Commit = rf.Committed_Idx
	// args.Entries = rf.Log_Array
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
	// var wg sync.WaitGroup
	// wg.Add(len(rf.peers) - 1)
	args := RequestVoteArgs{
		Candidate_Curr_Term: rf.CurrentTerm,
		Candidate_Id:        rf.me,
		// Last_Log_Index:      last_idx,
		// Last_Log_Term: rf.Log_Array[last_idx].Log_Term,
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
					log.Println(rf.me, "send the rpc to the ", idx)
					// to do
					if rf.CurrentTerm == args.Candidate_Curr_Term && rf.Raft_Status == Candidate {
						log.Println("pos here")
						if reply.Vote_Granted {
							vote_cnt++
							log.Println(rf.me, "get vote numver is", vote_cnt)
							if vote_cnt > len(rf.peers)/2 {
								rf.ToLeader()
								rf.StartHeart()
								log.Println("heart sync finish, change ", rf.me, " to leader")
							}
						} else if reply.Current_Term > rf.CurrentTerm {
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
	// flag := false
	// for idx, entry := range rf.Log_Array {
	// 	if entry.Log_Term == args.PrevLogTerm && idx == args.PrevLogIndex {
	// 		flag = true
	// 		break
	// 	}
	// }
	// if !flag {
	// 	reply.Success = false
	// 	return
	// }
	// // 以上是为了参与者和自己的日志相同，如果不是则拒绝
	// // 确认一致的最后一条日志，然后删除这条之后的，再添加
	// if len(rf.Log_Array) > args.PrevLogIndex && rf.Log_Array[args.PrevLogIndex].Log_Term == args.PrevLogTerm {
	// 	rf.Log_Array = rf.Log_Array[:args.PrevLogIndex+1]
	// }
	// // to d
	// rf.Log_Array = append(rf.Log_Array, args.Entries[args.PrevLogIndex+1:]...)
	// if args.Leader_Commit > rf.Committed_Idx {
	// 	rf.Committed_Idx = int(math.Min(float64(args.Leader_Commit), float64(len(rf.Log_Array))))
	// }
	rf.ToFollower()
	log.Println("append success", rf.me, "  ", time.Now())
	rf.ResetElection()
	rf.CurrentTerm = args.Leader_Term
	reply.Success = true
	//TODO:2,3,4,5 in the paper

}
