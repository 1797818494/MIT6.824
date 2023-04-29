package shardctrler

import (
	"fmt"
	"log"
	"math"
	"sort"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {

	if Debug {
		log.Printf(format, a...)
	}
	return
}

type StateMachine struct {
	Configs []Config
}

func Group2Shards(config *Config) map[int][]int {
	g2s := make(map[int][]int)
	if len(config.Groups) >= 1 {
		//为了保障节点配置相同，map顺序访问
		min_key := math.MaxInt
		for key := range config.Groups {
			if key < min_key {
				min_key = key
			}
		}
		DPrintf("one group and gid is {%v}", min_key)
		for index, gid := range config.Shards {
			if gid == 0 {
				config.Shards[index] = min_key
			}
		}
	}
	for group_id := range config.Groups {
		g2s[group_id] = make([]int, 0)
	}
	for shared, group := range config.Shards {
		g2s[group] = append(g2s[group], shared)
	}
	return g2s
}

func (sm *StateMachine) Join(gid_servers map[int][]string) {
	DPrintf("join {%v}", gid_servers)
	latest_config := sm.Configs[len(sm.Configs)-1]
	new_config := Config{len(sm.Configs), latest_config.Shards, deep_copy(latest_config.Groups)}
	for gid, servers := range gid_servers {
		if _, ok := new_config.Groups[gid]; !ok {
			new_servers := make([]string, len(servers))
			copy(new_servers, servers)
			new_config.Groups[gid] = new_servers
		}

	}
	g2s := Group2Shards(&new_config)
	for {
		min_gid, max_gid := GetGIDWithMinNumShards(g2s), GetGIDWithMaxNumShards(g2s)
		DPrintf("min_gid{%v} max_gid{%v}", min_gid, max_gid)
		if len(g2s[max_gid])-len(g2s[min_gid]) <= 1 {
			break
		}
		g2s[min_gid] = append(g2s[min_gid], g2s[max_gid][0])
		g2s[max_gid] = g2s[max_gid][1:]
	}
	var Shards [NShards]int
	for gid, shards := range g2s {
		for _, shard := range shards {
			DPrintf("shared{%v} -----> gid{%v}", shard, gid)
			Shards[shard] = gid
		}
	}
	new_config.Shards = Shards
	DPrintf("join group len{%v}", len(new_config.Groups))
	sm.Configs = append(sm.Configs, new_config)
}

func (sm *StateMachine) Leave(gids []int) {
	DPrintf("leave %v", gids)
	config_last_copy := sm.Configs[len(sm.Configs)-1]
	newConfig := Config{len(sm.Configs), config_last_copy.Shards, deep_copy(config_last_copy.Groups)}
	g2s := Group2Shards(&newConfig)
	orphanShards := make([]int, 0)
	for _, gid := range gids {
		delete(newConfig.Groups, gid)

		if shards, ok := g2s[gid]; ok {
			orphanShards = append(orphanShards, shards...)
			delete(g2s, gid)
		}
	}
	var newShards [NShards]int
	if len(newConfig.Groups) != 0 {
		for _, shard := range orphanShards {
			target := GetGIDWithMinNumShards(g2s)
			g2s[target] = append(g2s[target], shard)
		}
		for gid, shards := range g2s {
			for _, shard := range shards {
				newShards[shard] = gid
			}
		}
	}
	newConfig.Shards = newShards
	sm.Configs = append(sm.Configs, newConfig)

}

func deep_copy(map_args map[int][]string) map[int][]string {
	result_map := make(map[int][]string)
	for key, value := range map_args {
		result_map[key] = append(result_map[key], value...)
	}
	return result_map
}
func GetGIDWithMinNumShards(group_to_shards map[int][]int) int {
	// 为了使map迭代的顺序一致
	var keys []int
	for k := range group_to_shards {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	index, min := -1, NShards+1
	for _, gid := range keys {
		if gid != 0 && len(group_to_shards[gid]) < min {
			index, min = gid, len(group_to_shards[gid])
		}
	}
	return index
}

func GetGIDWithMaxNumShards(group_to_shards map[int][]int) int {
	var keys []int
	for k := range group_to_shards {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	index, max := -1, 0
	for _, gid := range keys {
		if gid != 0 && len(group_to_shards[gid]) > max {
			index, max = gid, len(group_to_shards[gid])
		}
	}
	return index
}
func (sm *StateMachine) Query(num int) Config {
	DPrintf("query{%v}", num)
	if num == -1 || num >= len(sm.Configs) {
		return sm.Configs[len(sm.Configs)-1]
	}
	return sm.Configs[num]
}

func (sm *StateMachine) Move(shard int, gid int) {
	DPrintf("move{%v}  {%v}", shard, gid)
	config_last_copy := sm.Configs[len(sm.Configs)-1]
	config_last_copy.Num += 1
	config_last_copy.Shards[shard] = gid
	sm.Configs = append(sm.Configs, config_last_copy)
	// config := Config{len(sm.configs), shard, sm.configs[len(sm.configs)-1].Groups}
}

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	lastApplied        int
	clientsInformation map[int64]ClientInfo
	configs            []Config // indexed by config num
	notify_chan        map[int]chan *ReplyRpc
	state_machine      StateMachine
}
type ClientInfo struct {
	Last_commandId int
	Last_repy      ReplyRpc
}

func (sc *ShardCtrler) applylogtoState(op Op) *ReplyRpc {
	return_reply := new(ReplyRpc)
	if op.Op_type == query_op {
		return_reply.Config = sc.state_machine.Query(op.Num)
		DPrintf("Node{%v} reply group{%v}", sc.me, return_reply.Config.Groups)
	}
	if op.Op_type == join_op {
		sc.state_machine.Join(op.Servers)
	}
	if op.Op_type == leave_op {
		sc.state_machine.Leave(op.Gids)
	}
	if op.Op_type == move_op {
		sc.state_machine.Move(op.Shard, op.Gid)
	}
	return_reply.Err = OK
	return return_reply
}

func (sc *ShardCtrler) applier() {
	for {
		DPrintf("Node{%v} here applier", sc.me)
		select {
		case message := <-sc.applyCh:
			{
				DPrintf("Node{%v} try to applymessage{%v}", sc.me, message)
				if message.CommandValid {
					sc.mu.Lock()
					if message.CommandIndex <= sc.lastApplied {
						DPrintf("Node{%v} command{%v} is less than kv lastapply{%v}", sc.me, message.CommandIndex, sc.lastApplied)
						sc.mu.Unlock()
						continue
					}
					sc.lastApplied = message.CommandIndex
					reply := new(ReplyRpc)
					make_op := message.Command.(Op)
					DPrintf("Node{%v} make op{%v}", sc.me, make_op)
					if sc.isdupicate(make_op.ClientId, make_op.CommandId) {
						DPrintf("Node{%v} is duplicate", sc.me)
						reply.Err = sc.clientsInformation[make_op.ClientId].Last_repy.Err
					} else {
						reply = sc.applylogtoState(make_op)
						sc.clientsInformation[make_op.ClientId] = ClientInfo{make_op.CommandId, *reply}
					}
					current_term, is_Leader := sc.rf.GetState()
					if is_Leader && message.CommandTerm == current_term {
						DPrintf("Node{%v} get state", sc.me)
						notify_chan := sc.newChannel(message.CommandIndex)
						notify_chan <- reply
						DPrintf("reply to notify chan{%v}", reply)
					} else {
						DPrintf("Node{%v} is not leader", sc.me)
					}
					sc.mu.Unlock()
				} else {
					panic(fmt.Sprintf("valid message{%v}", message))
				}
			}

		}
	}
}

func (sc *ShardCtrler) newChannel(commandIdx int) chan *ReplyRpc {
	_, ok := sc.notify_chan[commandIdx]
	if !ok {
		sc.notify_chan[commandIdx] = make(chan *ReplyRpc, 1)
	}
	return sc.notify_chan[commandIdx]
}

func (sc *ShardCtrler) isdupicate(clientId int64, commandIdx int) bool {
	return sc.clientsInformation[clientId].Last_commandId >= commandIdx
}

type Op struct {
	// Your data here.
	ClientId  int64
	Op_type   int
	CommandId int
	Servers   map[int][]string // join
	Gids      []int            //leave
	Shard     int              //move
	Gid       int
	Num       int //query
}

func (sc *ShardCtrler) Join(args *RequestRpc, reply *ReplyRpc) {
	// Your code here.
	sc.Process(args, reply)
}

func (sc *ShardCtrler) Leave(args *RequestRpc, reply *ReplyRpc) {
	// Your code here.
	sc.Process(args, reply)
}

func (sc *ShardCtrler) Move(args *RequestRpc, reply *ReplyRpc) {
	// Your code here.
	sc.Process(args, reply)
}

func (sc *ShardCtrler) Query(args *RequestRpc, reply *ReplyRpc) {
	// Your code here.
	sc.Process(args, reply)
}

func (sc *ShardCtrler) Process(args *RequestRpc, reply *ReplyRpc) {
	make_op := Op{ClientId: args.ClientId, Op_type: args.Op_type, CommandId: args.CommandId}
	if make_op.Op_type == query_op {
		make_op.Num = args.Num
	}
	if make_op.Op_type == leave_op {
		make_op.Gids = args.GIDs
	}
	if make_op.Op_type == join_op {
		make_op.Servers = args.Servers
	}
	if make_op.Op_type == move_op {
		make_op.Shard = args.Shared
		make_op.Gid = args.GID
	}
	DPrintf("Node{%v} start process", sc.me)
	sc.mu.Lock()
	if sc.isdupicate(make_op.ClientId, make_op.CommandId) {
		lastreply := sc.clientsInformation[make_op.ClientId].Last_repy
		reply.Err = lastreply.Err
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()
	DPrintf("node{%v} stall here", sc.me)
	index, _, is_leader := sc.rf.Start(make_op)
	if !is_leader {
		reply.Err = ErrWrongLeader
		return
	}
	sc.mu.Lock()
	ch := sc.newChannel(index)
	sc.mu.Unlock()
	select {
	case rpc := <-ch:
		reply.Err = rpc.Err
		if make_op.Op_type == query_op {
			reply.Config = rpc.Config
		}
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrTimeOut
	}
	DPrintf("Node{%v} will reply{%v}", sc.me, reply)
	sc.mu.Lock()
	sc.Delete(index)
	sc.mu.Unlock()
}

func (sc *ShardCtrler) Delete(commandIdx int) {
	delete(sc.notify_chan, commandIdx)
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.

func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me
	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	labgob.Register(ClientInfo{})
	labgob.Register(StateMachine{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	sc.lastApplied = 0
	sc.notify_chan = make(map[int]chan *ReplyRpc)
	sc.clientsInformation = make(map[int64]ClientInfo)
	sc.state_machine.Configs = append(sc.state_machine.Configs, sc.configs...)
	// Your code here.
	go sc.applier()
	return sc
}
