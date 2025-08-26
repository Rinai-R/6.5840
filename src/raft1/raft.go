package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

// A Go object implementing a single Raft peer.

const (
	Follower  = 0
	Leader    = 1
	Candidate = 2
)

type Raft struct {
	mu                sync.Mutex          // Lock to protect shared access to this peer's state
	peers             []*labrpc.ClientEnd // RPC end points of all peers
	persister         *tester.Persister   // Object to hold this peer's persisted state
	me                int                 // this peer's index into peers[]
	dead              int32               // set by Kill()
	currentTerm       int                 // 当前已知的最新的任期
	leaderIdx         int                 // 当前 leader 的索引
	voteFor           int                 // 当前投票给的候选人
	lastHeartBeatTime time.Time           // 上次心跳时间
	outTime           time.Duration       // 心跳超时时间
	role              int
	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() {
		return rf.currentTerm, false
	}
	return rf.currentTerm, rf.me == rf.leaderIdx
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	if rf.killed() {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	// 如果对方的任期更旧
	if args.Term < rf.currentTerm {
		// 直接拒绝
		return
	}

	// 如果任期对方比自己更早
	if args.Term > rf.currentTerm {
		// 一切状态都要更新
		rf.currentTerm = args.Term
		rf.voteFor = -1
		rf.leaderIdx = -1
		rf.role = Follower
		reply.Term = rf.currentTerm
	}
	// 只有在没有投票的时候才会投票
	if rf.voteFor == -1 || rf.voteFor == args.CandidateId {
		rf.lastHeartBeatTime = time.Now()
		rf.voteFor = args.CandidateId
		reply.VoteGranted = true
		// fmt.Printf("第%v任期：%v投票给%v\n", rf.currentTerm, rf.me, args.CandidateId)
		return
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term      int
	LeaderIdx int
}

type AppendEntriesReply struct {
	Term      int
	LeaderIdx int
	Success   bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.killed() {
		reply.Success = true
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.LeaderIdx = rf.leaderIdx

	// 如果旧任期发来心跳
	if args.Term < rf.currentTerm {
		return
	}
	// 此时说明心跳是最新的，我们可能需要更新状态
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.voteFor = -1
	}
	// 更新一些状态。
	rf.leaderIdx = args.LeaderIdx
	rf.role = Follower
	rf.lastHeartBeatTime = time.Now()

	reply.Term = rf.currentTerm
	reply.LeaderIdx = rf.leaderIdx
	reply.Success = true
	// fmt.Printf("第%v任期：%v向%v发起心跳\n", rf.currentTerm, rf.leaderIdx, rf.me)
}

func (rf *Raft) sendHeartBeat(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		// fmt.Printf("%v监听\n", rf.me)

		// 这里加锁读取
		rf.mu.Lock()
		role := rf.role
		leader := rf.leaderIdx
		timeout := rf.lastHeartBeatTime.Add(rf.outTime).Before(time.Now())
		rf.mu.Unlock()

		// 没领导的时候或超时的时候，如果还没有给别人投票并且自己是 follower
		if (leader == -1 || timeout) && role != Leader {
			rf.StartElection()
		}
		if leader == rf.me {
			rf.SendHeartBeat()
		}
		ms := 50 + (rand.Int63() % 200) // 50~250ms tick
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) StartElection() {
	// 先给自己投票
	rf.mu.Lock()
	rf.role = Candidate
	rf.currentTerm += 1
	rf.voteFor = rf.me
	rf.leaderIdx = -1
	rf.lastHeartBeatTime = time.Now()
	rf.mu.Unlock()
	// 构造参数
	args := &RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
	}
	term := rf.currentTerm
	voteNum := &atomic.Int64{}
	quorum := len(rf.peers) / 2
	me := rf.me

	// 请求投票
	for idx := range rf.peers {
		if idx == me {
			continue
		}
		// 用 goroutine 是为了防止有的节点挂了导致回复超时。
		go func(server int) {
			var reply RequestVoteReply
			ok := rf.sendRequestVote(server, args, &reply)
			if !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			// 这个表示上一任选拔还残留的 goroutine，直接扔了。
			if rf.currentTerm != term || rf.role != Candidate {
				return
			}
			if reply.Term > rf.currentTerm {
				// 看到更高term，立刻降级
				rf.currentTerm = reply.Term
				rf.role = Follower
				rf.leaderIdx = -1
				rf.voteFor = -1
				// 防止立刻又去选举
				rf.lastHeartBeatTime = time.Now()
				return
			}
			if reply.VoteGranted {
				voteNum.Add(1)
				if int(voteNum.Load()) >= quorum &&
					rf.role == Candidate && rf.currentTerm == term {

					// 当选Leader
					rf.role = Leader
					rf.leaderIdx = me
					// 立刻异步发一波心跳
					go rf.SendHeartBeat()
				}
			}
		}(idx)
	}

}

func (rf *Raft) SendHeartBeat() {
	rf.mu.Lock()
	term := rf.currentTerm
	me := rf.me
	rf.mu.Unlock()
	// 构造参数
	args := &AppendEntriesArgs{
		Term:      term,
		LeaderIdx: me,
	}
	for i := range rf.peers {
		if i == me {
			continue
		}
		// goroutine 理由同上。
		go func(server int) {
			var reply AppendEntriesReply
			ok := rf.sendHeartBeat(server, args, &reply)
			if !ok {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			// 若对方回了更高term，立刻降级
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.role = Follower
				rf.leaderIdx = reply.LeaderIdx
				rf.voteFor = -1
				rf.lastHeartBeatTime = time.Now()
			}
		}(i)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.currentTerm = 0
	rf.leaderIdx = -1
	rf.voteFor = -1
	rf.outTime = 600 * time.Millisecond
	rf.role = Follower
	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
