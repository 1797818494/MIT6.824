package shardkv

import (
	"fmt"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type KVstorage struct {
	KV map[string]string
}

func NewKVstorage() *KVstorage {
	return &KVstorage{make(map[string]string)}
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

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	lastApplied        int
	storage            Storage
	clientsInformation map[int64]ClientInfo
	notify_chann       map[int]chan *CommandReply
}

type ClientInfo struct {
	last_commandId int
	last_repy      CommandReply
}

type CommandReply struct {
	Value string
	Err   Err
}

type Storage interface {
	Get(Key string) (string, Err)
	Put(Key, Value string) Err
	Append(Key, Value string) Err
}

func (kv *ShardKV) newChannel(commandIdx int) chan *CommandReply {
	kv.notify_chann[commandIdx] = make(chan *CommandReply)
	return kv.notify_chann[commandIdx]
}
func (kv *ShardKV) Delete(commandIdx int) {
	delete(kv.notify_chann, commandIdx)
}
func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	make_op := Op{
		ClientId:  args.ClientId,
		OpType:    GET,
		Key:       args.Key,
		Value:     "",
		CommandId: args.CommandId_G,
	}
	kv.mu.Unlock()
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
		reply.Value, reply.Err = rpc.Value, rpc.Err
	case <-time.After(1 * time.Second):
		reply.Err = ErrTimeOut
	}
	kv.mu.Lock()
	kv.Delete(index)
	kv.mu.Unlock()
	kv.rf
	// Your code here.
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
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
		Value:     "",
		CommandId: args.CommandId_PA,
	}
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
	go func() {
		kv.mu.Lock()
		kv.Delete(index)
		kv.mu.Unlock()
	}()
}
func (kv *ShardKV) Killed() bool {
	return false
}

func (kv *ShardKV) isdupicate(clientId int64, commandIdx int) bool {
	return kv.clientsInformation[clientId].last_commandId >= commandIdx
}
func (kv *ShardKV) applylogtoState(op Op) *CommandReply {
	return_reply := new(CommandReply)
	if op.OpType == GET {
		return_reply.Value, return_reply.Err = kv.storage.Get(op.Key)
	}
	if op.OpType == APPEND {
		return_reply.Err = kv.storage.Append(op.Key, op.Value)
	}
	if op.OpType == PUT {
		return_reply.Err = kv.storage.Put(op.Key, op.Value)
	}
	return return_reply
}
func (kv *ShardKV) applier() {
	for {
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
					if make_op.OpType != GET && kv.isdupicate(make_op.ClientId, make_op.CommandId) {
						DPrintf("Node{%v} is duplicate", kv.me)
						reply.Err = kv.clientsInformation[make_op.ClientId].last_repy.Err
					} else {
						reply = kv.applylogtoState(make_op)
						if make_op.OpType != GET {
							kv.clientsInformation[make_op.ClientId] = ClientInfo{last_commandId: make_op.CommandId, last_repy: *reply}
						}
					}
					if _, is_Leader := kv.rf.GetState(); is_Leader {
						notify_chan := kv.newChannel(message.CommandIndex)
						notify_chan <- reply
					}
					kv.mu.Unlock()
				} else {
					panic(fmt.Sprintf("Valid message{%v}", message))
				}
			}
		}
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers
	kv.storage = NewKVstorage()
	kv.lastApplied = 0
	// 3A
	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	go kv.applier()
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	return kv
}
