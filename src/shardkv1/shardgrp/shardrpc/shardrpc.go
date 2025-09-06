package shardrpc

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
)

type FreezeShardArgs struct {
	Shard shardcfg.Tshid
	Num   shardcfg.Tnum
}

type FreezeShardReply struct {
	State []byte
	Num   shardcfg.Tnum
	Err   rpc.Err
}

type InstallShardArgs struct {
	Shard shardcfg.Tshid
	State []byte
	Num   shardcfg.Tnum
}

type InstallShardReply struct {
	Err rpc.Err
}

type DeleteShardArgs struct {
	Shard shardcfg.Tshid
	Num   shardcfg.Tnum
}

type DeleteShardReply struct {
	Err rpc.Err
}

type PutArgs struct {
	Key      string
	Value    string
	Version  rpc.Tversion
	ClientId int64
	ReqId    int64
}

type PutReply struct {
	Err rpc.Err
}

type GetArgs struct {
	Key      string
	ClientId int64
	ReqId    int64
}

type GetReply struct {
	Value   string
	Version rpc.Tversion
	Err     rpc.Err
}
