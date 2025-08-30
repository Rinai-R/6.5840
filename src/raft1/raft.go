package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"

	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
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
	mu                sync.Mutex            // Lock to protect shared access to this peer's state
	peers             []*labrpc.ClientEnd   // RPC end points of all peers
	persister         *tester.Persister     // Object to hold this peer's persisted state
	me                int                   // this peer's index into peers[]
	dead              int32                 // set by Kill()
	currentTerm       int                   // 当前已知的最新的任期
	leaderIdx         int                   // 当前 leader 的索引
	voteFor           int                   // 当前投票给的候选人
	lastHeartBeatTime time.Time             // 上次心跳时间
	outTime           time.Duration         // 心跳超时时间
	role              int                   // 角色，0 为 follower，1 为 leader，2 为 candidate
	logs              []LogEntry            // 日志
	commitIdx         int                   // 已提交的日志索引
	lastAppliedIdx    int                   // 该节点最后 apply 日志的索引
	nextIndex         []int                 // 对于每一个服务器，下一次发送日志的索引
	matchIndex        []int                 // 对于每一个服务器，已知的最新的日志索引
	applyCh           chan raftapi.ApplyMsg // 用于通知 leader 节点 apply 日志
	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
}

type LogEntry struct {
	Term    int
	Command interface{}
}

func (rf *Raft) getLastLogIndex() int {
	return len(rf.logs) - 1
}

func (rf *Raft) termAt(index int) int {
	if len(rf.logs) <= index || index < 0 {
		return 0
	}
	return rf.logs[index].Term
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

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.logs)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var voteFor int
	var logs []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&logs) != nil {
		DPrintf("error restoring state from stable storage")
	}
	rf.currentTerm = currentTerm
	rf.voteFor = voteFor
	rf.logs = logs
	lastIdx := rf.getLastLogIndex()
	for i := range rf.peers {
		rf.nextIndex[i] = lastIdx + 1
		rf.matchIndex[i] = 0
	}
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
	// changed 统一做持久化
	changed := false
	defer func() {
		if changed {
			rf.persist()
		}
	}()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	// 如果对方的任期更旧
	if args.Term < rf.currentTerm {
		// 直接拒绝
		return
	}

	myIdx := rf.getLastLogIndex()
	myTerm := rf.termAt(myIdx)
	// fmt.Printf("判断条件：%v > %v || (%v == %v && %v >= %v)\n", args.LastLogTerm, myTerm, args.LastLogTerm, myTerm, args.LastLogIndex, myIdx)
	if !(args.LastLogTerm > myTerm ||
		(args.LastLogTerm == myTerm && args.LastLogIndex >= myIdx)) {
		// 日志不够新，拒绝投票
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		changed = true
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
		changed = true
	}
	// 只有在没有投票的时候才会投票
	if rf.voteFor == -1 || rf.voteFor == args.CandidateId {
		rf.lastHeartBeatTime = time.Now()
		rf.voteFor = args.CandidateId
		changed = true
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
	Term         int
	LeaderIdx    int
	Entries      []LogEntry
	PrevLogIdx   int
	PrevLogTerm  int
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	LeaderIdx     int
	Success       bool
	ConflictTerm  int
	ConflictIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.killed() {
		reply.Success = true
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

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

	if args.PrevLogIdx > rf.getLastLogIndex() {
		// term 为 -1 表示缺少日志的错误。
		// fmt.Println("缺少日志")
		reply.ConflictTerm = -1
		reply.ConflictIndex = rf.getLastLogIndex() + 1
		return
	}

	if args.PrevLogTerm != rf.termAt(args.PrevLogIdx) {
		t := rf.termAt(args.PrevLogIdx)
		// fmt.Printf("节点%v和leader%v日志在term%v分叉\n", rf.me, args.LeaderIdx, t)
		// 此时表示有分叉，leader 只需要 term 即可
		first := args.PrevLogIdx
		// 找到这个 term 的起点，需要由 leader 覆盖
		for first > 0 && rf.termAt(first-1) == t {
			first--
		}
		reply.ConflictTerm = t
		reply.ConflictIndex = first
		return
	}

	// 这里开始追加日志
	idx := args.PrevLogIdx + 1
	// fmt.Printf("节点%v追加日志：%v，当前日志%v\n", rf.me, args.Entries, rf.logs)

	// fmt.Printf("节点 %v 对 follower%v 追加操作：%v\n", args.LeaderIdx, rf.me, len(args.Entries))
	for i, entry := range args.Entries {

		// fmt.Printf("节点 %v 对 follower%v 追加操作：%v\n", args.LeaderIdx, rf.me, len(args.Entries))
		if idx+i <= rf.getLastLogIndex() {
			// 已有日志位置，检查 term 是否一致
			if rf.logs[idx+i].Term != entry.Term {
				// fmt.Printf("节点%v日志和leader%v在索引%v处term%v冲突，leader覆盖\n", rf.me, args.LeaderIdx, idx+i, rf.logs[idx+i].Term)
				// term 不同，截断掉这条以及之后的日志
				rf.logs = rf.logs[:idx+i]
				// 把剩余 entries 整体追加
				rf.logs = append(rf.logs, args.Entries[i:]...)
				break
			}
			// 继续
		} else {
			// 剩下都是新的日志了
			rf.logs = append(rf.logs, args.Entries[i:]...)
			break
		}
	}

	// commitIdx推进用min(args.LeaderCommit, getLastLogIndex())
	if args.LeaderCommit > rf.commitIdx {
		last := rf.getLastLogIndex()
		if args.LeaderCommit < last {
			rf.commitIdx = args.LeaderCommit
		} else {
			rf.commitIdx = last
		}
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
	if rf.role != Leader || rf.killed() {
		return -1, -1, false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := len(rf.logs)
	term := rf.currentTerm
	isLeader := rf.me == rf.leaderIdx

	// fmt.Printf("%v：节点%v写入日志命令%v, 日志长度%v\n", rf.me == rf.leaderIdx, rf.me, command, rf.logs)

	rf.logs = append(rf.logs, LogEntry{
		Term:    term,
		Command: command,
	})
	rf.persist()
	// Your code here (3B).
	// if rf.role == Leader {
	// 	fmt.Printf("节点%v写入日志命令%v, 日志长度%v\n", rf.me, command, rf.logs)
	// }
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
		ms := 50 + (rand.Int63() % 300) // 50~350ms tick
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) StartElection() {
	// 先给自己投票
	rf.mu.Lock()
	rf.role = Candidate
	rf.currentTerm += 1
	rf.voteFor = rf.me
	rf.persist()
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
		// fmt.Printf("第%v任期：%v 发起选举，请求%v投票\n", rf.currentTerm, rf.me, idx)
		// 用 goroutine 是为了防止有的节点挂了导致回复超时。
		go func(server int) {
			args.LastLogIndex = rf.getLastLogIndex()
			args.LastLogTerm = rf.termAt(args.LastLogIndex)
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
				rf.persist()
				// 防止立刻又去选举
				rf.lastHeartBeatTime = time.Now()
				return
			}
			if reply.VoteGranted {
				voteNum.Add(1)
				// fmt.Printf("第%v任期：%v 发起选举，请求%v投票成功，持票数%v\n", rf.currentTerm, rf.me, idx, voteNum.Load())
				if int(voteNum.Load()) >= quorum &&
					rf.role == Candidate && rf.currentTerm == term {
					// fmt.Println("当leader了")
					// 当选Leader
					rf.role = Leader
					rf.leaderIdx = me
					for i := range rf.peers {
						if i == me {
							continue
						}
						rf.nextIndex[i] = rf.getLastLogIndex() + 1
						rf.matchIndex[i] = 0
					}
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
	quorum := len(rf.peers)/2 + 1
	rf.mu.Unlock()

	for i := range rf.peers {
		if i == me {
			continue
		}

		go func(server int) {
			// 用 for 循环的原因是保证立刻重试进行同步
			for {
				rf.mu.Lock()
				if rf.role != Leader || rf.killed() {
					rf.mu.Unlock()
					return
				}
				prevIdx := rf.nextIndex[server] - 1
				prevTerm := rf.termAt(prevIdx)
				entries := make([]LogEntry, len(rf.logs[rf.nextIndex[server]:]))
				copy(entries, rf.logs[rf.nextIndex[server]:])
				args := &AppendEntriesArgs{
					Term:         term,
					LeaderIdx:    me,
					PrevLogIdx:   prevIdx,
					PrevLogTerm:  prevTerm,
					Entries:      entries,
					LeaderCommit: rf.commitIdx,
				}
				rf.mu.Unlock()
				// 无锁 rpc 调用
				var reply AppendEntriesReply
				ok := rf.sendHeartBeat(server, args, &reply)
				if !ok {
					return
				}

				rf.mu.Lock()
				// 说明应该退化为 follower，不需要管同步的事情直接 return 就行。
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.role = Follower
					rf.leaderIdx = reply.LeaderIdx
					rf.voteFor = -1
					rf.persist()
					rf.lastHeartBeatTime = time.Now()
					rf.mu.Unlock()
					return
				}
				// 成功时更新状态，此时表示心跳，日志同步均成功。
				if reply.Success {
					match := args.PrevLogIdx + len(args.Entries)
					if match > rf.matchIndex[server] {
						rf.matchIndex[server] = match
					}
					ne := match + 1
					if ne > rf.nextIndex[server] {
						rf.nextIndex[server] = ne
					}
					// commitIdx 倒序遍历进行推进
					for N := len(rf.logs) - 1; N > rf.commitIdx; N-- {
						// 值得注意的一点，就是旧 term 就算被写入绝大多数节点，也不能视作“安全”
						if rf.logs[N].Term != rf.currentTerm {
							continue
						}
						cnt := 1
						for p := range rf.peers {
							if p == rf.me {
								continue
							}
							if rf.matchIndex[p] >= N {
								cnt++
							}
						}
						// 如果半数以上节点已经写入，则推进 commitIdx
						if cnt >= quorum {
							rf.commitIdx = N
							break
						}
					}
					rf.mu.Unlock()
					return
				}

				// -1 表示缺日志
				if reply.ConflictTerm == -1 {
					rf.nextIndex[server] = reply.ConflictIndex
				} else {
					// 不是 -1 并且 false 表示有冲突
					idx := reply.ConflictIndex
					// 基于返回的 idx（优化），如果有同样 term 的日志，则继续回退
					for idx > 0 && rf.termAt(idx-1) == reply.ConflictTerm {
						idx--
					}
					rf.nextIndex[server] = idx
				}
				rf.mu.Unlock()
				// for 循环没结束，说明同步失败，继续 for 循环，直到同步成功
			}
		}(i)
	}
}

func (rf *Raft) applyLoop(applyCh chan raftapi.ApplyMsg) {
	for !rf.killed() {
		rf.mu.Lock()
		var msgs []raftapi.ApplyMsg
		for rf.commitIdx > rf.lastAppliedIdx && rf.lastAppliedIdx < rf.getLastLogIndex() {
			rf.lastAppliedIdx += 1
			msgs = append(msgs, raftapi.ApplyMsg{
				CommandValid: true,
				Command:      rf.logs[rf.lastAppliedIdx].Command,
				CommandIndex: rf.lastAppliedIdx,
			})
		}
		// 这里必须在锁内apply，防止竞态
		for _, msg := range msgs {
			applyCh <- msg
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
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
	rf.outTime = 1000 * time.Millisecond
	rf.role = Follower
	rf.logs = make([]LogEntry, 0)
	rf.logs = append(rf.logs, LogEntry{
		Term:    0,
		Command: nil,
	})
	rf.applyCh = applyCh
	rf.commitIdx = 0
	rf.lastAppliedIdx = 0
	rf.lastHeartBeatTime = time.Now()
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.dead = 0
	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyLoop(applyCh)
	return rf
}
