package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

// import "bytes"
// import "../labgob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state
	currentTerm int // 已知的最新任期，初始化为0，单增
	votedFor int // 当前任期投给谁了
	logs []Log // 初始idx为1，日志条目，含状态机的命令，领导人接收该条目的任期
	// volatile state
	commitIndex int // 已知的已经commit的日志中最高的索引值，初始化为0，单增
	lastApplied int // 应用到状态机中的日志中最高的索引值，初始化为0，单增

	// volatile state for leader
	nextIndex []int // 对于每一台服务器，发送到该服务器的下一个日志条目的索引（初始值为leader最后的日志条目的索引+1）
	matchIndex []int // 对于每一台服务器，已知的已经复制到该服务器的最高日志条目的索引（初始值为0，单调递增）

	leaderID int // 记录谁是leader 初始化为-1
	state State // 当前server的状态
	electionTimeout time.Duration // 选举超时时钟时长
	appendNotify chan bool // 收到心跳通知管道，观察者模式
	voteNotify chan bool // 收到投票请求
	victoryNotify chan bool // candidate获胜，成为leader
	voterCnt int // leader数数自己还有多少个选民
}

type Log struct{
	Command string
	Term int
}

type State int
const(
	Follower = iota
	Candidate
	Leader
)


// return currentTerm and whether this server
// believes it is the leader.
// Your code here (2A).
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool

	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = rf.state == Leader

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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



//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int // 候选人的任期号
	CandidateId int // 请求选票的候选人的 ID
	LastLogIndex int // 候选人的最后日志条目的索引值
	LastLogTerm int // 候选人最后日志条目的任期号
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int // 当前任期号，以便于候选人去更新自己的任期号
	VoteGranted bool // 选人赢得了此张选票时为真
}

//
// example RequestVote RPC handler.
//
/*
1. 如果term < currentTerm返回 false （5.2 节）
2. 如果 votedFor 为空或者为 candidateId，并且候选人的日志至少和自己一样新，那么就投票给他（5.2 节，5.4 节）
*/
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.debug("某人想让我投票，是他： " + strconv.Itoa(args.CandidateId))

	reply.VoteGranted = false
	if args.Term < rf.currentTerm{
		return
	}

	if args.Term > rf.currentTerm{
		rf.turnFollower(args.Term, -1)
	}


	if rf.votedFor == -1 || rf.votedFor == args.CandidateId{
		if rf.logs[len(rf.logs)-1].Term < args.LastLogTerm ||
			(rf.logs[len(rf.logs)-1].Term == args.LastLogTerm) && len(rf.logs) <= args.LastLogIndex{
			rf.debug("我投了")
			reply.VoteGranted = true
			//rf.voteNotify <- true // 投了票
			addNotify(rf.voteNotify)
			rf.votedFor = args.CandidateId
			rf.debug("my vote: " + strconv.Itoa(rf.votedFor))

		}
	}

}

// 自己加的AppendEntries
type AppendEntriesArgs struct {
	Term int // 领导人的任期
	LeaderId int // 领导人 ID 因此跟随者可以对客户端进行重定向（译者注：跟随者根据领导人 ID 把客户端的请求重定向到领导人，比如有时客户端把请求发给了跟随者而不是领导人）
	PrevLogIndex int // 紧邻新日志条目之前的那个日志条目的索引
	PrevLogTerm int // 紧邻新日志条目之前的那个日志条目的任期
	Entries []Log // 需要被保存的日志条目（被当做心跳使用时，则日志条目内容为空；为了提高效率可能一次性发送多个）
	LeaderCommit int // 领导人的已知已提交的最高的日志条目的索引
}

type AppendEntriesReply struct {
	Term int //	当前任期，对于领导人而言 它会更新自己的任期
	Success bool // 如果跟随者所含有的条目和 prevLogIndex 以及 prevLogTerm 匹配上了，则为 true
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm{
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// 收到心跳
	rf.debug("收到心跳了++++++++++++++++++++++++++++")
	addNotify(rf.appendNotify)


	if args.Term > rf.currentTerm{
		rf.turnFollower(args.Term, args.LeaderId)
		rf.debug("转换&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&")
	}

	if rf.state == Candidate{
		rf.turnFollower(rf.currentTerm, args.LeaderId)
	}

	if args.PrevLogIndex == -1{ // 心跳包
		return
	}

	if args.PrevLogIndex < len(rf.logs) && rf.logs[args.PrevLogIndex].Term == args.PrevLogTerm {
		return
	}else{
		reply.Success = false
		return
	}


}


//
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
//

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}


//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.debug("I was killed.....................")
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// my add func
// 重置选举超时时钟时长
func (rf *Raft)resetElectionTimeout()  {
	rand.Seed(time.Now().UnixNano())
	rf.electionTimeout = time.Duration(300 + rand.Intn(200)) * time.Millisecond
}

func (rf *Raft)getServerState() State{
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state
}

func (rf *Raft)getVoterCnt() int{
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.voterCnt
}

// 状态机控制中心
func (rf *Raft)server(){
	for !rf.killed(){
		switch rf.getServerState(){
		case Follower:
			rf.follower()
		case Leader:
			rf.leader()
		case Candidate:
			rf.candidate()
		}
		rf.debug("我现在是：" + strconv.Itoa(int(rf.getServerState())))
	}
}

func (rf *Raft)follower(){
	// follower没有并发，没必要加lock
	rf.debug("I am a follower")
	//rf.debug("term: " + strconv.Itoa(rf.currentTerm) + " leader:  " + strconv.Itoa(rf.leaderID))
	electionTimer := time.NewTimer(rf.electionTimeout)
	//rf.debug(time.StampMilli)
	//rf.debug("timeout: " + strconv.Itoa(int(rf.electionTimeout)))
	select {
	case <-rf.appendNotify:
		// pass
	case <-rf.voteNotify:
		// pass
	case <-electionTimer.C:
		// 前面两个都没发生，转变为Candidate
		rf.mu.Lock()
		rf.debug("成为候选人：" + time.StampMilli)
		rf.turnCandidate()
		rf.mu.Unlock()
	}
}

func (rf *Raft)leader(){
	rf.mu.Lock()
	rf.voterCnt = 0
	rf.mu.Unlock()

	rf.debug("I am a leader!!!!!!!!!!!!!!!!!!!")
	rf.broadcastAppendEntries()
	go func() {

		time.Sleep(50 * time.Millisecond)

		if rf.getVoterCnt() < len(rf.peers)/2{
			rf.mu.Lock()
			rf.turnFollower(rf.currentTerm, -1)
			rf.mu.Unlock()
		}
	}()
}

func addNotify(ch chan bool){
	go func() {
		ch <- true
	}()
}

func (rf *Raft)broadcastAppendEntries(){
	heartBeatTimeout := time.Duration(105 * time.Millisecond)
	time.Sleep(heartBeatTimeout)
	rf.debug("我在发心跳>>>>>>>>>>>>>>>")
	for i := 0; i < len(rf.peers); i++{
		if i == rf.me{ // 我知道自己的心还在跳
			continue
		}
		go func(target int) {
			// 心跳包
			rf.mu.Lock()
			args := AppendEntriesArgs{rf.currentTerm, rf.me, -1, -1, nil, -1}
			rf.mu.Unlock()
			reply := AppendEntriesReply{-1, false}
			rf.sendAppendEntries(target, &args, &reply)
			if reply.Term != -1{ // 收到回复了
				rf.mu.Lock()
				rf.voterCnt++
				rf.mu.Unlock()
			}
		}(i)
	}
}

func (rf *Raft)candidate(){
	rf.debug("I am a candidate")
	electionTimer := time.NewTimer(rf.electionTimeout)
	rf.pleaseVoteMe() // 没必要 go

	select {
	case <-electionTimer.C:
		// 此次选举超时，再次发起选举
		rf.debug("选举超时")
		rf.mu.Lock()
		rf.turnCandidate()
		rf.mu.Unlock()
	case <-rf.appendNotify:
		// 在append里已经转变状态了
	case <-rf.victoryNotify:
		// 接收到大多数服务器的选票，那么就变成领导人
		rf.mu.Lock()
		rf.turnLeader()
		rf.mu.Unlock()
	}
}

func (rf *Raft)turnFollower(targetTerm int, leaderID int){
	rf.currentTerm = targetTerm
	rf.state = Follower
	rf.votedFor = -1
	rf.leaderID = leaderID
}

func (rf *Raft)turnCandidate(){
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.resetElectionTimeout()
	rf.state = Candidate
}

func (rf *Raft)turnLeader(){
	rf.state = Leader
	rf.leaderID = rf.me
}




// 发送请求投票的 RPC 给其他所有服务器
func (rf *Raft)pleaseVoteMe()  {
	rf.debug("投我一票,,,,,,,,,,,,,,,,,")
	mutex := sync.Mutex{}
	votesNum := 0
	for i := 0; i < len(rf.peers); i++{
		if i == rf.me{
			continue
		}

		args := RequestVoteArgs{rf.currentTerm, rf.me, len(rf.logs),
								rf.logs[len(rf.logs)-1].Term}
		reply := RequestVoteReply{}
		// 不等待，让go routine自己去等待
		go func(target int) {
			ok := rf.sendRequestVote(target, &args, &reply)
			if rf.getServerState() == Candidate{
				if ok && reply.VoteGranted == true{
					mutex.Lock()
					votesNum++
					rf.debug("votesNum: " + strconv.Itoa(votesNum))
					if votesNum >= len(rf.peers)/2 {
						rf.debug("enough!!")
						//rf.victoryNotify <- true
						addNotify(rf.victoryNotify)
					}
					mutex.Unlock()

				}
			}
		}(i)
	}


}


//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1 // 谁也没投
	rf.logs = append(rf.logs, Log{"", -1}) // 初始任期为1
	rf.debug(strconv.Itoa(rf.logs[len(rf.logs)-1].Term))
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.turnFollower(0, -1)
	rf.resetElectionTimeout()
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.appendNotify = make(chan bool)
	rf.voteNotify = make(chan bool)
	rf.victoryNotify = make(chan bool)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.debug("start server")
	// 需要立刻执行完，不等待，并发开启各个server
	go rf.server()


	return rf
}

func (rf *Raft)debug(str string){
	fmt.Printf("[%d]: " + str + "\n", rf.me)
}

