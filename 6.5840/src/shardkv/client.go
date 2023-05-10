package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
	"6.5840/shardctrler"
)

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	leaderIds map[int]int
	clientId  int64
	CommandId int
}

// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	ck.leaderIds = make(map[int]int)
	ck.clientId = nrand()
	ck.CommandId = 1
	ck.config = ck.sm.Query(-1)
	// You'll have to add code here.
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
func (ck *Clerk) Get(key string) string {
	DPrintf("get k{%v}", key)
	return ck.Command(&CommandRequest{Key: key, Op: OpGet})
	// args := GetArgs{}
	// args.Key = key

	// for {
	// 	shard := key2shard(key)
	// 	gid := ck.config.Shards[shard]
	// 	if servers, ok := ck.config.Groups[gid]; ok {
	// 		// try each server for the shard.
	// 		for si := 0; si < len(servers); si++ {
	// 			srv := ck.make_end(servers[si])
	// 			var reply GetReply
	// 			ok := srv.Call("ShardKV.Get", &args, &reply)
	// 			if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
	// 				return reply.Value
	// 			}
	// 			if ok && (reply.Err == ErrWrongGroup) {
	// 				break
	// 			}
	// 			// ... not ok, or ErrWrongLeader
	// 		}
	// 	}
	// 	time.Sleep(100 * time.Millisecond)
	// 	// ask controler for the latest configuration.
	// 	ck.config = ck.sm.Query(-1)
	// }

	// return ""
}

// shared by Put and Append.
// // You will have to modify this function.
// func (ck *Clerk) PutAppend(key string, value string, op string) {
// 	args := PutAppendArgs{}
// 	args.Key = key
// 	args.Value = value
// 	args.Op = op

// 	for {
// 		shard := key2shard(key)
// 		gid := ck.config.Shards[shard]
// 		if servers, ok := ck.config.Groups[gid]; ok {
// 			for si := 0; si < len(servers); si++ {
// 				srv := ck.make_end(servers[si])
// 				var reply PutAppendReply
// 				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
// 				if ok && reply.Err == OK {
// 					DPrintf("put k{%v} v{%v} success", key, value)
// 					return
// 				}
// 				if ok && reply.Err == ErrWrongGroup {
// 					break
// 				}
// 				// ... not ok, or ErrWrongLeader
// 			}
// 		}
// 		time.Sleep(100 * time.Millisecond)
// 		// ask controler for the latest configuration.
// 		ck.config = ck.sm.Query(-1)
// 	}
// }

func (ck *Clerk) Command(request *CommandRequest) string {
	request.ClientId, request.CommandId = ck.clientId, ck.CommandId
	for {
		shard := key2shard(request.Key)
		gid := ck.config.Shards[shard]
		DPrintf("client gid{%v} to command", gid)
		if servers, ok := ck.config.Groups[gid]; ok {
			if _, ok = ck.leaderIds[gid]; !ok {
				ck.leaderIds[gid] = 0
			}
			oldLeaderId := ck.leaderIds[gid]
			newLeaderId := oldLeaderId
			for {
				var responce CommandResponce
				ok := ck.make_end(servers[newLeaderId]).Call("ShardKV.Command", request, &responce)
				if ok && (responce.Err == OK || responce.Err == ErrNoKey) {
					DPrintf("command success")
					ck.CommandId++
					return responce.Value
				} else if ok && responce.Err == ErrWrongGroup {
					DPrintf("wronggroup")
					break
				} else {
					newLeaderId = (newLeaderId + 1) % len(servers)
					DPrintf("leaderID{%v} {%v}", newLeaderId, responce.Err)
					if newLeaderId == oldLeaderId {
						break
					}
					continue
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		ck.config = ck.sm.Query(-1)
		DPrintf("config client is num{%v} shards{%v}, groups{%v}", ck.config.Num, ck.config.Shards, ck.config.Groups)
	}
}

func (ck *Clerk) Put(key string, value string) {
	// ck.PutAppend(key, value, "Put")
	DPrintf("put k{%v} v{%v}", key, value)
	ck.Command(&CommandRequest{Key: key, Value: value, Op: OpPut})
}
func (ck *Clerk) Append(key string, value string) {
	// ck.PutAppend(key, value, "Append")
	DPrintf("append k{%v} v{%v}", key, value)
	ck.Command(&CommandRequest{Key: key, Value: value, Op: OpAppend})
}
