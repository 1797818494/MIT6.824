package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientId  int64
	commandId int
	LeaderId  int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clientId = nrand()
	ck.LeaderId = 0
	ck.commandId = 1
	// You'll have to add code here.
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.

func (ck *Clerk) Get(key string) string {
	DPrintf("Node{%v} start get{%v}", ck.clientId, key)
	args := GetArgs{
		Key:         key,
		Op:          "Get",
		LeaderId:    ck.LeaderId,
		CommandId_G: ck.commandId,
		ClientId:    ck.clientId,
	}
	for {
		var reply GetReply
		if !ck.servers[ck.LeaderId].Call("KVServer.Get", &args, &reply) || reply.Err == ErrWrongLeader || reply.Err == ErrTimeOut {
			ck.LeaderId = (ck.LeaderId + 1) % int64(len(ck.servers))
			// log.Println(1)
			continue
		}
		ck.commandId++
		DPrintf("Node{%v} start get succeed{%v}  value{%v}, commandId{%v}", ck.clientId, key, reply.Value, ck.commandId)
		return reply.Value
	}
	// You will have to modify this function.
	return ""
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{
		Key:          key,
		Value:        value,
		Op:           op,
		LeaderId:     ck.LeaderId,
		CommandId_PA: ck.commandId,
		ClientId:     ck.clientId,
	}
	DPrintf("Node{%v} start appendput{%v} value{%v} op{%v}", ck.clientId, key, args.Value, op)
	for {
		var reply PutAppendReply
		if !ck.servers[ck.LeaderId].Call("KVServer.PutAppend", &args, &reply) || reply.Err == ErrWrongLeader || reply.Err == ErrTimeOut {
			ck.LeaderId = (ck.LeaderId + 1) % int64(len(ck.servers))
			// log.Println(1)
			continue
		}
		ck.commandId++
		DPrintf("Node{%v} appendput sucess", ck.clientId)
		return
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
