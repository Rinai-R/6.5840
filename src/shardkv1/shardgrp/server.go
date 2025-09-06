package shardgrp

import (
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
	"github.com/bytedance/sonic"
)

const (
	GetOperation     = 1
	PutOperation     = 2
	FreezeOperation  = 3
	InstallOperation = 4
	DeleteOperation  = 5
)

const (
	Serving = 0
)

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM
	gid  tester.Tgid

	mu sync.Mutex // 用于 doOp 操作的原子性

	kvs map[string]*KeyValue

	last map[int64]*LastApplied // 用于去重

	Status int64 // 状态

	installTo shardcfg.Tnum
	deleteTo  shardcfg.Tnum
	freezeTo  shardcfg.Tnum

	frozenShard map[shardcfg.Tshid]bool
}

type LastApplied struct {
	LastAppliedId int64
	Reply         Reply
}

// 有锁条件下调用
func (kv *KVServer) IsDuplicate(cliendId int64, reqId int64) (any, bool) {
	if last, ok := kv.last[cliendId]; ok && last.LastAppliedId >= reqId {
		return last.Reply, true
	}
	return nil, false
}

// 有锁条件下调用
func (kv *KVServer) StoreLastApplied(cliendId int64, reqId int64, reply Reply) {
	kv.last[cliendId] = &LastApplied{
		LastAppliedId: reqId,
		Reply:         reply,
	}
}

type KeyValue struct {
	Key   string
	Value string
	Ver   rpc.Tversion
}

// DoOp 的请求参数和返回值
type Request struct {
	Type     int
	Key      string
	Value    string
	Version  rpc.Tversion
	ClientId int64
	ReqId    int64

	ConfigVersion shardcfg.Tnum
	ObjectShard   shardcfg.Tshid
	Data          []byte
}

type Reply struct {
	Key     string
	Value   string
	Version rpc.Tversion
	Err     rpc.Err

	Data          []byte
	ConfigVersion shardcfg.Tnum
}

// snapshot 操作的数据
type StateMachineData struct {
	KVs       map[string]*KeyValue
	installTo shardcfg.Tnum
	deleteTo  shardcfg.Tnum
	freezeTo  shardcfg.Tnum

	frozenShard map[shardcfg.Tshid]bool
	last        map[int64]*LastApplied
}

// freeze，install 和 delete 操作的数据。
type InstallData struct {
	KVs map[string]*KeyValue
}

func (kv *KVServer) DoOp(req any) any {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	Req := req.(Request)
	switch Req.Type {

	case GetOperation:
		var reply Reply

		// 需要判断 Key 对应的分区是否已经被冻结
		if kv.Frozen(Req.Key) {
			reply.Err = rpc.ErrMovingShard
			return reply
		}
		KV, ok := kv.kvs[Req.Key]
		if !ok {
			reply.Err = rpc.ErrNoKey
			return reply
		}
		reply.Value = KV.Value
		reply.Version = KV.Ver
		reply.Err = rpc.OK
		return reply

	case PutOperation:
		var reply Reply

		if rep, ok := kv.IsDuplicate(Req.ClientId, Req.ReqId); ok {
			return rep
		}
		// 需要判断 Key 对应的分区是否已经被冻结
		if kv.Frozen(Req.Key) {
			reply.Err = rpc.ErrMovingShard
			return reply
		}
		KV, ok := kv.kvs[Req.Key]
		// 没有数据
		if !ok {
			// 版本号不对
			if Req.Version != 0 {
				reply.Err = rpc.ErrNoKey
				kv.StoreLastApplied(Req.ClientId, Req.ReqId, reply)
				return reply
			}
			// 版本号为 0，表示新增键值。
			KV = &KeyValue{Req.Key, Req.Value, 1}
			kv.kvs[Req.Key] = KV
			reply.Err = rpc.OK
			kv.StoreLastApplied(Req.ClientId, Req.ReqId, reply)
			return reply
		}
		// fmt.Printf("Put Key= %s, Value= %s, Version= %d， clientid= %d, reqid= %d\n",
		// 	Req.Key, Req.Value, Req.Version, Req.ClientId, Req.ReqId)
		// 有数据，判断版本号是否对得上
		if Req.Version != KV.Ver {
			// fmt.Println("key= ", Req.Key, "版本错误！", Req.Version, "!=", KV.Ver)
			reply.Err = rpc.ErrVersion
			kv.StoreLastApplied(Req.ClientId, Req.ReqId, reply)
			return reply
		}
		// 版本号对得上，更新数据
		KV.Value = Req.Value
		KV.Ver += 1
		kv.kvs[Req.Key] = KV
		reply.Err = rpc.OK
		kv.StoreLastApplied(Req.ClientId, Req.ReqId, reply)
		return reply

	case FreezeOperation:
		var reply Reply
		if Req.ConfigVersion < kv.freezeTo {
			reply.Err = rpc.ErrStale
			return reply
		}
		kv.freezeTo = Req.ConfigVersion
		kv.frozenShard[Req.ObjectShard] = true
		reply.Err = rpc.OK
		reply.ConfigVersion = kv.freezeTo
		reply.Data = kv.GetShardData(Req.ObjectShard)
		return reply

	case InstallOperation:
		var reply Reply
		if Req.ConfigVersion < kv.installTo {
			reply.Err = rpc.ErrStale
			return reply
		}
		kv.installTo = Req.ConfigVersion
		kv.SetShardData(Req.Data)
		reply.Err = rpc.OK
		reply.ConfigVersion = kv.installTo
		return reply

	case DeleteOperation:
		var reply Reply
		if Req.ConfigVersion < kv.deleteTo {
			reply.Err = rpc.ErrStale
			return reply
		}
		kv.deleteTo = Req.ConfigVersion
		kv.DeleteShardKV(Req.ObjectShard)
		reply.Err = rpc.OK
		reply.ConfigVersion = kv.deleteTo
		return reply
	}
	return Reply{}
}

func (kv *KVServer) Frozen(Key string) bool {
	shard := shardcfg.Key2Shard(Key)
	ok := kv.frozenShard[shard]
	return ok
}

func (kv *KVServer) GetShardData(shard shardcfg.Tshid) []byte {
	kvs := make(map[string]*KeyValue)
	for k, v := range kv.kvs {
		if shardcfg.Key2Shard(k) == shard {
			kvs[k] = v
		}
	}
	srd := InstallData{
		KVs: kvs,
	}
	data, _ := sonic.Marshal(srd)
	return data
}

func (kv *KVServer) SetShardData(data []byte) {
	srd := InstallData{}
	sonic.Unmarshal(data, &srd)
	for k, v := range srd.KVs {
		if kv.Frozen(k) {
			delete(kv.frozenShard, shardcfg.Key2Shard(k))
		}
		kv.kvs[k] = v
	}
}

func (kv *KVServer) DeleteShardKV(shard shardcfg.Tshid) {
	for k := range kv.kvs {
		if shardcfg.Key2Shard(k) == shard {
			delete(kv.kvs, k)
		}
	}
}

func (kv *KVServer) Snapshot() []byte {
	smd := StateMachineData{
		KVs:         kv.kvs,
		installTo:   kv.installTo,
		deleteTo:    kv.deleteTo,
		freezeTo:    kv.freezeTo,
		frozenShard: kv.frozenShard,
		last:        kv.last,
	}
	data, _ := sonic.Marshal(smd)
	return data
}

func (kv *KVServer) Restore(data []byte) {
	smd := StateMachineData{
		KVs:       make(map[string]*KeyValue),
		installTo: 0,
		freezeTo:  0,
		deleteTo:  0,
		last:      make(map[int64]*LastApplied),
	}
	sonic.Unmarshal(data, &smd)
	kv.kvs = smd.KVs
	kv.installTo = smd.installTo
	kv.deleteTo = smd.deleteTo
	kv.freezeTo = smd.freezeTo
	kv.frozenShard = smd.frozenShard
	kv.last = smd.last
}

func (kv *KVServer) Get(args *shardrpc.GetArgs, reply *shardrpc.GetReply) {
	kv.mu.Lock()
	if kv.Frozen(args.Key) {
		kv.mu.Unlock()
		reply.Err = rpc.ErrMovingShard
		return
	}
	kv.mu.Unlock()
	req := Request{
		Type: GetOperation,
		Key:  args.Key,
	}
	err, replyValue := kv.rsm.Submit(req)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	reply.Value = replyValue.(Reply).Value
	reply.Version = replyValue.(Reply).Version
	reply.Err = replyValue.(Reply).Err
}

func (kv *KVServer) Put(args *shardrpc.PutArgs, reply *shardrpc.PutReply) {
	kv.mu.Lock()
	if kv.Frozen(args.Key) {
		kv.mu.Unlock()
		reply.Err = rpc.ErrMovingShard
		return
	}
	kv.mu.Unlock()
	req := Request{
		Type:     PutOperation,
		Key:      args.Key,
		Value:    args.Value,
		Version:  args.Version,
		ReqId:    args.ReqId,
		ClientId: args.ClientId,
	}
	// fmt.Println("hello")
	err, replyValue := kv.rsm.Submit(req)
	// fmt.Println("-----------hello")
	if err != rpc.OK {
		reply.Err = err
		return
	}
	reply.Err = replyValue.(Reply).Err
}

// Freeze the specified shard (i.e., reject future Get/Puts for this
// shard) and return the key/values stored in that shard.
func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {

	Req := Request{
		Type:          FreezeOperation,
		ConfigVersion: args.Num,
		ObjectShard:   args.Shard,
	}
	err, replyValue := kv.rsm.Submit(Req)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	reply.Num = replyValue.(Reply).ConfigVersion
	reply.State = replyValue.(Reply).Data
	reply.Err = replyValue.(Reply).Err
}

// Install the supplied state for the specified shard.
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {

	Req := Request{
		Type:          InstallOperation,
		ConfigVersion: args.Num,
		ObjectShard:   args.Shard,
		Data:          args.State,
	}
	err, replyValue := kv.rsm.Submit(Req)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	reply.Err = replyValue.(Reply).Err
}

// Delete the specified shard.
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {

	Req := Request{
		Type:          DeleteOperation,
		ConfigVersion: args.Num,
		ObjectShard:   args.Shard,
	}
	err, replyValue := kv.rsm.Submit(Req)
	if err != rpc.OK {
		reply.Err = err
		return
	}

	reply.Err = replyValue.(Reply).Err
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
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartShardServerGrp starts a server for shardgrp `gid`.
//
// StartShardServerGrp() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartServerShardGrp(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(shardrpc.FreezeShardArgs{})
	labgob.Register(shardrpc.InstallShardArgs{})
	labgob.Register(shardrpc.DeleteShardArgs{})
	labgob.Register(rsm.Op{})
	labgob.Register(shardrpc.PutArgs{})
	labgob.Register(shardrpc.GetArgs{})
	labgob.Register(Request{})
	labgob.Register(Reply{})

	kv := &KVServer{
		gid:         gid,
		me:          me,
		kvs:         make(map[string]*KeyValue),
		mu:          sync.Mutex{},
		last:        make(map[int64]*LastApplied),
		frozenShard: make(map[shardcfg.Tshid]bool),
	}
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	// Your code here

	return []tester.IService{kv, kv.rsm.Raft()}
}
