package shardgrp

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt        *tester.Clnt
	servers     []string
	maxQueryNum int
	// You will have to modify this struct.
	cacheLeaderIdx int
	clientId       int64
	requestId      int64
}

func MakeClerk(clnt *tester.Clnt, servers []string) *Clerk {
	ck := &Clerk{
		clnt:           clnt,
		servers:        servers,
		maxQueryNum:    len(servers) * 2,
		cacheLeaderIdx: 0,
		clientId:       0,
		requestId:      0,
	}
	return ck
}

func (ck *Clerk) SetClientId(clientId int64) {
	ck.clientId = clientId
}

func (ck *Clerk) SetRequestId(requestId int64) {
	ck.requestId = requestId
}

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {

	i := ck.cacheLeaderIdx
	defer func() {
		ck.cacheLeaderIdx = i
	}()
	args := shardrpc.GetArgs{
		Key:      key,
		ClientId: ck.clientId,
		ReqId:    ck.requestId,
	}
	reply := shardrpc.GetReply{}
	var ok bool
	var tried int = 0
	for reply.Err == rpc.ErrWrongLeader || !ok {
		ok = ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)
		// if !ok {
		// 	fmt.Println("请求Get", reply)
		// }
		if reply.Err == rpc.ErrWrongLeader || !ok {
			i = (i + 1) % len(ck.servers)
		}
		tried++
		if reply.Err != rpc.OK && tried >= ck.maxQueryNum {
			return "", 0, rpc.ErrWrongGroup
		}
	}
	return reply.Value, reply.Version, reply.Err
}

func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	i := ck.cacheLeaderIdx
	defer func() {
		ck.cacheLeaderIdx = i
	}()
	args := shardrpc.PutArgs{
		Key:      key,
		Value:    value,
		Version:  version,
		ClientId: ck.clientId,
		ReqId:    ck.requestId,
	}
	reply := shardrpc.PutReply{}
	var ok bool
	var tried int = 0
	for reply.Err == rpc.ErrWrongLeader || !ok {
		ok = ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
		// fmt.Println("请求Put", reply)
		if reply.Err == rpc.ErrWrongLeader || !ok {
			i = (i + 1) % len(ck.servers)
		}
		tried++
		if reply.Err != rpc.OK && tried >= ck.maxQueryNum {
			return rpc.ErrWrongGroup
		}
	}
	return reply.Err
}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	i := ck.cacheLeaderIdx
	defer func() {
		ck.cacheLeaderIdx = i
	}()
	req := shardrpc.FreezeShardArgs{}
	req.Shard = s
	req.Num = num
	reply := shardrpc.FreezeShardReply{}
	var ok bool
	for {

		ok = ck.clnt.Call(ck.servers[i], "KVServer.FreezeShard", &req, &reply)
		switch {
		case reply.Err == rpc.ErrWrongGroup:
			return nil, reply.Err
		case reply.Err == rpc.ErrWrongLeader || !ok:
			i = (i + 1) % len(ck.servers)
		default:
			return reply.State, reply.Err
		}
	}
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	i := ck.cacheLeaderIdx
	defer func() {
		ck.cacheLeaderIdx = i
	}()
	req := shardrpc.InstallShardArgs{}
	req.Shard = s
	req.State = state
	req.Num = num
	reply := shardrpc.InstallShardReply{}
	var ok bool
	for {
		ok = ck.clnt.Call(ck.servers[i], "KVServer.InstallShard", &req, &reply)
		switch {
		case reply.Err == rpc.ErrWrongLeader || !ok:
			i = (i + 1) % len(ck.servers)
		default:
			return reply.Err
		}
	}
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	req := shardrpc.DeleteShardArgs{}
	req.Shard = s
	req.Num = num
	reply := shardrpc.DeleteShardReply{}
	var ok bool
	for {
		ok = ck.clnt.Call(ck.servers[ck.cacheLeaderIdx], "KVServer.DeleteShard", &req, &reply)
		switch {
		case reply.Err == rpc.ErrWrongLeader || !ok:
			ck.cacheLeaderIdx = (ck.cacheLeaderIdx + 1) % len(ck.servers)

		default:
			return reply.Err
		}
	}
}
