package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	clientId  int64
	commandId int
	leaderId  int64
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
	ck.leaderId = 0
	ck.commandId = 1
	// Your code here.
	return ck
}

const (
	query_op = 1
	join_op  = 2
	leave_op = 3
	move_op  = 4
)

type RequestRpc struct {
	LeaderId  int64
	CommandId int
	ClientId  int64
	Op_type   int
	NumId     int
	Servers   map[int][]string
	GIDs      []int
	Shared    int
	GID       int
}

type ReplyRpc struct {
	Err    Err
	Config Config
}

func (ck *Clerk) Query(num int) Config {
	args := &RequestRpc{LeaderId: ck.leaderId, CommandId: ck.commandId, ClientId: ck.clientId, Op_type: query_op}
	// Your code here.
	args.NumId = num
	for {
		// try each known server.
		var reply ReplyRpc
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Query", args, &reply)
		if ok && reply.Err == OK {
			ck.commandId++
			DPrintf("Node{%v}  success", ck.clientId)
			return reply.Config
		}
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeOut {
			ck.leaderId = (ck.leaderId + 1) % int64(len(ck.servers))
			continue
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &RequestRpc{LeaderId: ck.leaderId, CommandId: ck.commandId, ClientId: ck.clientId, Op_type: join_op}
	// Your code here.
	args.Servers = servers

	for {
		// try each known server.
		var reply ReplyRpc
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Join", args, &reply)
		if ok && reply.Err == OK {
			ck.commandId++
			DPrintf("Node{%v}  success", ck.clientId)
			return
		}
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeOut {
			ck.leaderId = (ck.leaderId + 1) % int64(len(ck.servers))
			continue
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &RequestRpc{LeaderId: ck.leaderId, CommandId: ck.commandId, ClientId: ck.clientId, Op_type: leave_op}
	// Your code here.
	args.GIDs = gids

	for {
		// try each known server.
		var reply ReplyRpc
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Leave", args, &reply)
		if ok && reply.Err == OK {
			ck.commandId++
			DPrintf("Node{%v}  success", ck.clientId)
			return
		}
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeOut {
			ck.leaderId = (ck.leaderId + 1) % int64(len(ck.servers))
			continue
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &RequestRpc{LeaderId: ck.leaderId, CommandId: ck.commandId, ClientId: ck.clientId, Op_type: move_op}
	// Your code here.
	args.Shared = shard
	args.GID = gid

	for {
		// try each known server.
		var reply ReplyRpc
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Move", args, &reply)
		if ok && reply.Err == OK {
			ck.commandId++
			DPrintf("Node{%v} move success", ck.clientId)
			return
		}
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeOut {
			ck.leaderId = (ck.leaderId + 1) % int64(len(ck.servers))
			continue
		}
		time.Sleep(100 * time.Millisecond)
	}
}
