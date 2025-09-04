package rsm

import (
	"sync"
	"time"

	"github.com/bwmarrin/snowflake"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

var useRaftStateMachine bool // to plug in another raft besided raft1

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Me  int
	Id  int64
	Req any
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	// Your definitions here.
	resMap map[int64]chan any // map from op id to result
	node   *snowflake.Node
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	sf, _ := snowflake.NewNode(int64(me))
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		resMap:       make(map[int64]chan any),
		node:         sf,
	}
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	go rsm.ApplyMsg()
	return rsm
}

func (rsm *RSM) ApplyMsg() {
	for msg := range rsm.applyCh {
		rsm.mu.Lock()
		if msg.CommandValid {
			op := msg.Command.(Op)
			res := rsm.sm.DoOp(op.Req)
			// fmt.Println("RSM: got apply msg for op", op.Id, "res", res)
			if _, ok := rsm.resMap[op.Id]; !ok {
				rsm.mu.Unlock()
				continue
			}
			ch := rsm.resMap[op.Id]
			delete(rsm.resMap, op.Id)
			go func() {
				ch <- res
			}()
		} else if msg.SnapshotValid {
			// fmt.Println("恢复snapshot")
			rsm.sm.Restore(msg.Snapshot)
		}
		rsm.mu.Unlock()
	}
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.
	rsm.mu.Lock()
	me := rsm.me
	id := rsm.node.Generate().Int64()

	op := Op{
		Req: req,
		Me:  me,
		Id:  id,
	}
	ch := make(chan any, 1)
	rsm.resMap[id] = ch

	rsm.mu.Unlock()

	_, startTerm, isLeader := rsm.Raft().Start(op)
	if !isLeader {
		return rpc.ErrWrongLeader, nil
	}

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	// defer fmt.Printf("%v 退出\n", rsm.me)
	for {
		// fmt.Printf("%v 等待\n", rsm.me)
		select {
		case res := <-ch:
			return rpc.OK, res
		case <-ticker.C:
			curTerm, curLeader := rsm.rf.GetState()
			// 只要任期变了或已经不是 leader，就放弃本次等待
			if curTerm != startTerm || !curLeader {
				rsm.mu.Lock()
				if cur, ok := rsm.resMap[op.Id]; ok && cur == ch {
					delete(rsm.resMap, op.Id)
				}
				rsm.mu.Unlock()
				return rpc.ErrWrongLeader, nil
			}
		}
	}
}
