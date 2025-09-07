package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"log"
	"sync"

	kvsrv "6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	tester "6.5840/tester1"
)

const (
	ConfigCurrent = "config:current"
	ConfigNext    = "config:next"
)

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk

	killed int32 // set by Kill()

	Init *sync.Once
	// Your data here.
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{
		clnt: clnt,
		Init: &sync.Once{},
	}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	// Your code here.
	return sck
}

// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.
func (sck *ShardCtrler) InitController() {
	for {
		// 和 ChangeConfigTo() 的思路差不多，这里先检查 next 的版本号。
		curData, curVer, err := sck.Get(ConfigCurrent)
		if err != rpc.OK {
			log.Printf("get current failed: %v", err)
			continue
		}

		next, _, err := sck.Get(ConfigNext)
		if err != rpc.OK {
			log.Printf("get next failed: %v", err)
			continue
		}
		if next == "" {
			return
		}

		new := shardcfg.FromString(string(next))

		cur := shardcfg.FromString(string(curData))

		if new.Num <= cur.Num {
			return
		}
		// 迁移前再次确认 current 还在我们看到的版本
		_, curVer2, err := sck.Get(ConfigCurrent)
		if err != rpc.OK {
			continue
		}
		if curVer2 != curVer {
			// 有人抢先了，重试
			continue
		}

		oldCfg := shardcfg.FromString(string(curData))
		targetVer := shardcfg.Tnum(curVer + 1)
		sck.MigrateData(new, oldCfg, targetVer)

		err = sck.Put(ConfigCurrent, new.String(), curVer)
		if err == rpc.ErrMaybe {
			// 不确定是否成功，重试
			continue
		}
		if err == rpc.OK {
			// log.Printf("Changed config to version %d", curVer+1)
			// 仅仅在这里才会返回
			return
		}
	}
}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	sck.Init.Do(func() {
		sck.Put(ConfigCurrent, cfg.String(), 0)
		sck.Put(ConfigNext, "", 0)
	})
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	for {
		// 分别读取 current/next 的“版本锁”
		curData, curVer, err := sck.Get(ConfigCurrent)
		if err != rpc.OK {
			log.Printf("get current failed: %v", err)
			continue
		}
		_, nextVer, err := sck.Get(ConfigNext)
		if err != rpc.OK {
			log.Printf("get next failed: %v", err)
			continue
		}

		err = sck.Put(ConfigNext, new.String(), nextVer)
		if err == rpc.ErrMaybe {
			continue
		}
		if err != rpc.OK {
			continue
		}

		// 迁移前再次确认 current 还在我们看到的版本
		_, curVer2, err := sck.Get(ConfigCurrent)
		if err != rpc.OK {
			continue
		}
		if curVer2 != curVer {
			// 有人抢先了，重试
			continue
		}

		oldCfg := shardcfg.FromString(string(curData))
		targetVer := shardcfg.Tnum(curVer + 1)
		sck.MigrateData(new, oldCfg, targetVer)

		err = sck.Put(ConfigCurrent, new.String(), curVer)
		if err == rpc.ErrMaybe {
			// 不确定是否成功，重试
			continue
		}
		if err == rpc.OK {
			// log.Printf("Changed config to version %d", curVer+1)
			// 仅仅在这里才会返回
			return
		}

		// 出错了，重试
	}
}

// 迁移数据，遍历 shard 观察所属 group
func (sck *ShardCtrler) MigrateData(new *shardcfg.ShardConfig, old *shardcfg.ShardConfig, ver shardcfg.Tnum) {
	var i shardcfg.Tshid
	for i = 0; i < shardcfg.NShards; i++ {
		newGid := new.Shards[i]
		oldGid := old.Shards[i]
		if newGid == oldGid {
			continue
		}
		// 迁移数据
		sck.MigrateShard(i, newGid, oldGid, new, old, ver)
	}
}

// 将 shard 从一个group 迁移到另一个 group。
// shard 类似 redis 里面的 hash slot。
func (sck *ShardCtrler) MigrateShard(
	shard shardcfg.Tshid, newGid, oldGid tester.Tgid, newcfg, oldcfg *shardcfg.ShardConfig, ver shardcfg.Tnum) {
	newClerk := shardgrp.MakeClerk(sck.clnt, newcfg.Groups[newGid])
	oldClerk := shardgrp.MakeClerk(sck.clnt, oldcfg.Groups[oldGid])

	data, err := oldClerk.FreezeShard(shard, ver)
	if err != rpc.OK {
		log.Printf("Failed to freeze shard %d: %v", shard, err)
		return
	}

	err = newClerk.InstallShard(shard, data, ver)
	if err != rpc.OK {
		log.Printf("Failed to install shard %d: %v", shard, err)
	}

	// fmt.Println("Migrate shard", string(data))
	err = oldClerk.DeleteShard(shard, ver)
	if err != rpc.OK {
		log.Printf("Failed to delete shard %d: %v", shard, err)
	}
}

// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	data, _, err := sck.Get(ConfigCurrent)
	if err != rpc.OK {
		log.Printf("Query failed: %v", err)
		return nil
	}
	cfg := shardcfg.FromString(string(data))
	return cfg
}
