package shardkv

import (
	"bytes"
	"fmt"
	"log"
	"sync"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type ClientInfo struct {
	CommandId int
	Reply     *CommandResponce
}

type CommandReply struct {
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	sc           *shardctrler.Clerk
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big
	// Your definitions here.
	lastapplied      int
	lastconfig       shardctrler.Config
	currentConfig    shardctrler.Config
	stateMachines    map[int]*Shard
	clientInfomation map[int64]ClientInfo
	notify_chan      map[int]chan *CommandResponce
}

type CommandResponce struct {
	Value string
	Err   Err
}

type CommandType uint8
type Command struct {
	Op   CommandType
	Data interface{}
}

const (
	Operation CommandType = iota
	Configuration
	InsertShards
	DeleteShards
	EmptyEntry
)

type ShardStatus uint8

const (
	Serving ShardStatus = iota
	Pulling
	BePulling
	Gcing
)

type Shard struct {
	KV     map[string]string
	Status ShardStatus
}

func (kv *ShardKV) newChannel(commandIdx int) chan *CommandResponce {
	_, ok := kv.notify_chan[commandIdx]
	if !ok {
		kv.notify_chan[commandIdx] = make(chan *CommandResponce, 1)
	}
	return kv.notify_chan[commandIdx]
}

func (kv *ShardKV) Delete(commandIdx int) {
	delete(kv.notify_chan, commandIdx)
}

func NewShard() *Shard {
	return &Shard{make(map[string]string), Serving}
}

func (shard *Shard) Get(key string) (string, Err) {
	if value, ok := shard.KV[key]; ok {
		return value, OK
	}
	return "", ErrNoKey
}

func (shard *Shard) Put(key, value string) Err {
	shard.KV[key] = value
	return OK
}

func (shard *Shard) Append(key, value string) Err {
	shard.KV[key] += value
	return OK
}

func (shard *Shard) deep_copy() map[string]string {
	newShard := make(map[string]string)
	for k, v := range shard.KV {
		newShard[k] = v
	}
	return newShard
}

type Optype uint8

const (
	OpGet Optype = iota
	OpPut
	OpAppend
)

type CommandRequest struct {
	Key       string
	Op        Optype
	ClientId  int64
	CommandId int
}

func (kv *ShardKV) isDuplicated(clientId int64, commandId int) bool {
	return kv.clientInfomation[clientId].CommandId >= commandId
}

func (kv *ShardKV) canServer(shardID int) bool {
	return kv.currentConfig.Shards[shardID] == kv.gid && (kv.stateMachines[shardID].Status == Serving || kv.stateMachines[shardID] == Gcing)
}

func (kv *ShardKV) Command(request *CommandRequest, responce *CommandResponce) {
	kv.mu.Lock()
	if request.Op != OpGet && kv.isDuplicated(request.ClientId, request.CommandId) {
		lastResponce := kv.clientInfomation[request.ClientId].Reply
		responce.Err = lastResponce.Err
		kv.mu.Unlock()
		return
	}
	if !kv.canServer(key2shard(request.Key)) {
		responce.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	kv.Execute(NewOperationCommand(request), responce)

}

type ShardOperationResponce struct {
}
type ShardOperationRequest struct {
	ConfigNum int
	ShardIDs  []int
}

func NewOperationCommand(request *CommandRequest) Command {
	return Command{Operation, *request}
}

func NewConfigurationCommand(config *shardctrler.Config) Command {
	return Command{Configuration, *config}
}
func NewInsertSHardsCommand(response *ShardOperationResponce) Command {
	return Command{InsertShards, *response}
}
func NewDeleteShardsCommand(response *ShardOperationResponce) Command {
	return Command{DeleteShards, *response}
}
func NewEmptyEntryCommand() Command {
	return Command{EmptyEntry, nil}
}
func (kv *ShardKV) Execute(command Command, responce *CommandResponce) {

}
func (kv *ShardKV) applyOperation(message *raft.ApplyMsg, operation *CommandRequest) *CommandResponce {
	var responce *CommandResponce
	shardID := key2shard(operation.Key)
	if kv.canServer(shardID) {
		if operation.Op != OpGet && kv.isDuplicated(operation.ClientId, operation.CommandId) {
			return kv.clientInfomation[operation.ClientId].Reply
		} else {
			responce = kv.applyLogToStateMachine(operation, shardID)
			if operation.Op != OpGet {
				kv.clientInfomation[operation.ClientId] = ClientInfo{operation.CommandId, responce}
			}
			return responce
		}
	}
	return &CommandResponce{ErrWrongGroup, ""}
}

func (kv *ShardKV) applyLogToStateMachine(operation *CommandRequest, shardID int) CommandResponce {

}

func (kv *ShardKV) applier() {
	for {
		select {
		case message := <-kv.applyCh:
			DPrintf("Node{%v} Group{%v} try to apply message{%v}", kv.me, kv.gid, message)
			if message.CommandValid {
				kv.mu.Lock()
				if message.CommandIndex <= kv.lastapplied {
					DPrintf("Node{%v} Group{%v} commandIdx{%v} <= lastapplied{%v}", kv.me, kv.gid, message.CommandIndex, kv.lastapplied)
					kv.mu.Unlock()
					continue
				}
				kv.lastapplied = message.CommandIndex
				var responce *CommandResponce
				command := message.Command.(Command)
				switch command.Op {
				case Operation:
					operation := command.Data.(CommandRequest)
					responce = kv.applyOperation(&message, &operation)
				case Configuration:
					nextConfig := command.Data.(shardctrler.Config)
					responce = kv.applyConfigShards(&nextConfig)
				case InsertShards:
					shardsInfo := command.Data.(ShardOperationResponce)
					responce = kv.applyInsertShards(&shardsInfo)
				case DeleteShards:
					shardsInfo := command.Data.(ShardOperationResponce)
					responce = kv.applyDeleteShards(&shardsInfo)
				case EmptyEntry:
					responce = kv.applyEmptyEntry()
				}

				if current_term, isLeader := kv.rf.GetState(); isLeader && message.CommandTerm == current_term {
					ch := kv.newChannel(message.CommandIndex)
					ch <- responce
					DPrintf("responce{%v} notify chan", responce)
				} else {
					DPrintf("Node{%v} is no leader", kv.me)
				}

				if kv.rf.ShouldSnap(kv.maxraftstate, message.SnapshotIndex) {
					DPrintf("Node{%v} start to snapshot index{%v}", kv.me, message.CommandIndex)
					kv.snapMake(message.CommandIndex)
				}
				kv.mu.Unlock()

			} else if message.SnapshotValid {
				kv.mu.Lock()
				kv.storSnapshot(message.Snapshot)
				kv.lastapplied = message.SnapshotIndex
				kv.mu.Unlock()
			} else {
				panic(fmt.Sprintf("valid message{%v}", message))
			}
		}
	}
}

func (kv *ShardKV) getShardIDsByStatus(shard_status ShardStatus) map[int][]int {
	g2s := make(map[int][]int)
	for g, shardsID := range kv.lastconfig.Shards {
		g2s[g] = append(g2s[g], shardsID)
	}
	return g2s
}
func (kv *ShardKV) migrationAction() {
	kv.mu.Lock()
	g2s := kv.getShardIDsByStatus(Pulling)
	var wg sync.WaitGroup
	for gid, shardsIDs := range g2s {
		DPrintf("Node{%v} start pulltask")
		wg.Add(1)
		go func(servers []string, configNum int, shardIDs []int) {
			defer wg.Done()
			pullTaskRequest := ShardOperationRequest{configNum, shardIDs}
			for _, server := range servers {
				var pullTaskResponse ShardOperationResponce
				srv := kv.make_end(server)
				if srv.Call("ShardKV.GetShardsData", &pullTaskRequest, &pullTaskResponse) {
					DPrintf("Node{%v} get pulltaskResponce")
					kv.Execute(NewInsertSHardsCommand(&pullTaskResponse), &CommandResponce{})
				}
			}
		}(kv.lastconfig.Groups[gid], kv.currentConfig.Num, shardsIDs)
	}
	kv.mu.Unlock()
	wg.Wait()
}

func (kv *ShardKV) snapMake(snapIndex int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.clientInfomation)
	e.Encode(kv.storage)
	kv.rf.Snapshot(snapIndex, w.Bytes())
}

func (kv *ShardKV) storeSnapshot(snapshot []byte) {
	if len(snapshot) < 1 || snapshot == nil || kv.maxraftstate == -1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	client_information := make(map[int64]ClientInfo)
	if d.Decode(&client_information) != nil || d.Decode(&kv.storage) != nil {
		log.Fatalf("Node{%v} failed storeSnapshot", kv.me)
	}
	kv.clientInfomation = client_information
	// log.Printf("node{%v} snapdecode key0 {%v}", kv.me, kv.storage.KV["0"])
}

func (kv *ShardKV) configureAction() {
	canPerformNextConfig := true
	kv.mu.Lock()
	for _, shard := range kv.stateMachines {
		if shard.Status != Serving {
			canPerformNextConfig = false
			DPrintf("node{%v} can't apply new configuration", kv.me)
		}
	}
	currentConfigNum := kv.currentConfig.Num
	kv.mu.Unlock()
	if canPerformNextConfig {
		nextConfig := kv.sc.Query(currentConfigNum + 1)
		if nextConfig.Num == currentConfigNum+1 {
			DPrintf("node{%v} fetch the new configuration", kv.me)
			kv.Execute(NewConfigurationCommand(&nextConfig), &CommandResponce{})
		}

	}
}

func (kv *ShardKV) applyConfiguration(nextConfig *shardctrler.Config) *CommandResponce {
	if nextConfig.Num == kv.currentConfig.Num+1 {
		kv.updateShardStatus(nextConfig)
		kv.lastconfig = kv.currentConfig
		kv.currentConfig = *nextConfig
		return &CommandResponce{OK, ""}
	}
	return &CommandResponce{ErrOutDated, ""}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
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

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	return kv
}
