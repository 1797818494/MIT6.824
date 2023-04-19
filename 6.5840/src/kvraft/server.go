package kvraft

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVstorage struct {
	KV map[string]string
}

func NewKVstorage() *KVstorage {
	return &KVstorage{make(map[string]string)}
}

type Storage interface {
	Get(Key string) (string, Err)
	Put(Key, Value string) Err
	Append(Key, Value string) Err
}

func (storage *KVstorage) Get(Key string) (string, Err) {
	if value, ok := storage.KV[Key]; ok {
		return value, OK
	}
	return "", ErrNoKey
}

func (storage *KVstorage) Put(Key string, Value string) Err {
	storage.KV[Key] = Value
	return OK
}
func (storage *KVstorage) Append(Key string, Value string) Err {
	storage.KV[Key] += Value
	return OK
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId  int64
	OpType    int
	Key       string
	Value     string
	CommandId int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	lastApplied        int
	storage            KVstorage
	clientsInformation map[int64]ClientInfo
	notify_chann       map[int]chan *CommandReply
}
type ClientInfo struct {
	Last_commandId int
	Last_repy      CommandReply
}

type CommandReply struct {
	Value string
	Err   Err
}

func (kv *KVServer) isdupicate(clientId int64, commandIdx int) bool {
	return kv.clientsInformation[clientId].Last_commandId >= commandIdx
}
func (kv *KVServer) applylogtoState(op Op) *CommandReply {
	return_reply := new(CommandReply)
	if op.OpType == GET {
		return_reply.Value, return_reply.Err = kv.storage.Get(op.Key)
		DPrintf("kv receive Get(reply{%v})", return_reply)
	}
	if op.OpType == APPEND {
		return_reply.Err = kv.storage.Append(op.Key, op.Value)
	}
	if op.OpType == PUT {
		return_reply.Err = kv.storage.Put(op.Key, op.Value)
	}
	DPrintf("..............................")
	DPrintf("Node{%v} the return reply used {%v}", kv.me, return_reply)
	DPrintf("..............................")
	return return_reply
}
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	make_op := Op{
		ClientId:  args.ClientId,
		OpType:    GET,
		Key:       args.Key,
		Value:     "",
		CommandId: args.CommandId_G,
	}
	DPrintf("Node{%v} stall here", kv.me)
	index, _, is_leader := kv.rf.Start(make_op)
	if !is_leader {
		DPrintf("leader isn't Node{%v} ", kv.me)
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("leader apply makeop(%v)", make_op)
	kv.mu.Lock()
	ch := kv.newChannel(index)
	kv.mu.Unlock()
	select {
	case rpc := <-ch:
		reply.Value, reply.Err = rpc.Value, rpc.Err
	case <-time.After(2 * time.Second):
		reply.Err = ErrTimeOut
	}
	// kv.mu.Lock()
	// kv.Delete(index)
	// kv.mu.Unlock()

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	var op_type int
	if args.Op == "Append" {
		op_type = APPEND
	} else {
		op_type = PUT
	}
	make_op := Op{
		ClientId:  args.ClientId,
		OpType:    op_type,
		Key:       args.Key,
		Value:     args.Value,
		CommandId: args.CommandId_PA,
	}
	DPrintf("Node{%v} may be ........", kv.me)
	kv.mu.Lock()
	if kv.isdupicate(make_op.ClientId, make_op.CommandId) {
		lastreply := kv.clientsInformation[make_op.ClientId].Last_repy
		reply.Err = lastreply.Err
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	DPrintf("Node{%v} stall here", kv.me)
	index, _, is_leader := kv.rf.Start(make_op)
	if !is_leader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	ch := kv.newChannel(index)
	kv.mu.Unlock()
	select {
	case rpc := <-ch:
		reply.Err = rpc.Err
	case <-time.After(1 * time.Second):
		reply.Err = ErrTimeOut
	}
	DPrintf("Node{%v} here will reply{%v}", kv.me, reply)
	// kv.mu.Lock()
	// kv.Delete(index)
	// kv.mu.Unlock()

}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.storage = *NewKVstorage()
	kv.lastApplied = 0
	kv.notify_chann = make(map[int]chan *CommandReply)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.clientsInformation = make(map[int64]ClientInfo)
	kv.storeSnapshot(kv.rf.ReadSnapshot())
	go kv.applier()
	// You may need initialization code here.

	return kv
}

func (kv *KVServer) applier() {
	for !kv.killed() {
		DPrintf("Node{%v} here applier", kv.me)
		select {
		case message := <-kv.applyCh:
			{
				DPrintf("Node{%v} try to applymessage{%v}", kv.me, message)
				if message.CommandValid {
					kv.mu.Lock()
					if message.CommandIndex <= kv.lastApplied {
						DPrintf("Node{%v} command{%v} is less than kv lastapply{%v}", kv.me, message.CommandIndex, kv.lastApplied)
						kv.mu.Unlock()
						continue
					}
					kv.lastApplied = message.CommandIndex
					reply := new(CommandReply)
					make_op := message.Command.(Op)
					DPrintf("Node{%v} makeop.........{%v}", kv.me, make_op)
					if make_op.OpType != GET && kv.isdupicate(make_op.ClientId, make_op.CommandId) {
						DPrintf("Node{%v} is duplicate", kv.me)
						reply.Err = kv.clientsInformation[make_op.ClientId].Last_repy.Err
					} else {
						reply = kv.applylogtoState(make_op)
						if make_op.OpType != GET {
							kv.clientsInformation[make_op.ClientId] = ClientInfo{Last_commandId: make_op.CommandId, Last_repy: *reply}
						}
						kv.rf.Persist(kv.clientsInformation, kv.storage)
					}
					// kv.rf.Persist(kv.clientsInformation, kv.lastApplied)
					current_term, is_Leader := kv.rf.GetState()
					if is_Leader && message.CommandTerm == current_term {
						DPrintf("Node{%v} get state", kv.me)
						notify_chan := kv.newChannel(message.CommandIndex)
						notify_chan <- reply
						DPrintf("reply to notify chan{%v}", reply)
					} else {
						DPrintf("Node{%v} is not leader", kv.me)
					}
					// 3B
					if kv.rf.ShouldSnap(kv.maxraftstate, message.SnapshotIndex) {
						DPrintf("Node{%v} start to snapshot index{%v}", kv.me, message.CommandIndex)
						kv.snapMake(message.CommandIndex)
					}
					kv.mu.Unlock()
				} else if message.SnapshotValid {
					kv.mu.Lock()
					if kv.rf.CondInstallSnapshot(message.SnapshotIndex, message.SnapshotTerm, message.Snapshot) {
						kv.storeSnapshot(message.Snapshot)
						kv.lastApplied = message.SnapshotIndex
					}
					kv.mu.Unlock()
				} else {
					panic(fmt.Sprintf("Valid message{%v}", message))
				}
			}
		}
	}
}

// func (kv *KVServer) needSnap() bool {
// 	DPrintf("max raftstate{%v}   currentsize{%v}", kv.maxraftstate, raft.MakePersister().RaftStateSize())
// 	return kv.maxraftstate <= raft.MakePersister().RaftStateSize()
// }

func (kv *KVServer) snapMake(snapIndex int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.clientsInformation)
	e.Encode(kv.storage)
	kv.rf.Snapshot(snapIndex, w.Bytes())
}

func (kv *KVServer) storeSnapshot(snapshot []byte) {
	if len(snapshot) < 1 || snapshot == nil {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	client_information := make(map[int64]ClientInfo)
	if d.Decode(&client_information) != nil || d.Decode(&kv.storage) != nil {
		log.Fatalf("Node{%v} failed storeSnapshot", kv.me)
	}
	kv.clientsInformation = client_information
	DPrintf("success store")
}
func (kv *KVServer) newChannel(commandIdx int) chan *CommandReply {
	_, ok := kv.notify_chann[commandIdx]
	if !ok {
		kv.notify_chann[commandIdx] = make(chan *CommandReply, 1)
	}
	return kv.notify_chann[commandIdx]
}
func (kv *KVServer) Delete(commandIdx int) {
	delete(kv.notify_chann, commandIdx)
}
