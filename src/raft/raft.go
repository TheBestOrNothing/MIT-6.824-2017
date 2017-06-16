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
	"bytes"
	"encoding/gob"
	"fmt"
	"labrpc"
	"math"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// The log entery struct
//
type Entry struct {
	Term    int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	sync.Mutex                     // Lock to protect shared access to this peer's state
	peers      []*labrpc.ClientEnd // RPC end points of all peers
	persister  *Persister          // Object to hold this peer's persisted state
	me         int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	//LAB 2A
	kill        bool
	status      int           //The status of raft: leader, candidate, or follower
	currentTerm int           //Latest term server has seen
	votedFor    int           //CandidateID that receive vote in current term
	log         map[int]Entry //log
	f2c         *time.Timer   //Timer to kick off leader election
	heartbeatT  *time.Timer   //Ticker to triger heartbeat
	c2l         chan bool     //Channel for victory in votting
	c2f         chan bool     //Channel for victory in votting
	//LAB 2B - Volatile state on all servers
	commitIndex int
	lastApplied int

	//LAB 2B - volatile state on leaders
	//for each server, index of the next log entry to send to that server
	//(initialized to leader last log index + 1)
	nextIndex []int
	//for each server, index of highest log entry known to be replicated on server
	matchIndex []int
	//entriesChan chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.Lock()
	defer rf.Unlock()

	// Your code here (2A).
	return rf.currentTerm, rf.status == Leader

}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	//LAB 2A
	Term         int //candidate term
	CandidateID  int //candidate who is requesting vote
	LastLogTerm  int //term of candidate's last log entry
	LastLogIndex int //index of candidate's last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  //currentTerm from voter and for candidate to update itself
	VoteGranted bool //true means candidate receive the vote
}

//
// example RequestAppend RPC arguments structure.
// field names must start with capital letters!
//
type RequestAppendArgs struct {
	// Your data here (2A, 2B).
	//LAB 2A
	Term     int     //leader's term
	LeaderID int     //leader's id,so follower can redirect clients
	Entries  []Entry //log entries to store(empty for heartbeat)
	//LAB 2B
	//LAB 2B
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
}

//
// example RequestAppend RPC reply structure.
// field names must start with capital letters!
//
type RequestAppendReply struct {
	// Your data here (2A).
	Term    int //currentTerm, for leader to update itself
	Success bool
	//true if follower contained entry matching
	//prevLogIndex and prevLogTerm
}

//
// Is candidate's log up to date ? (5.4.1)
//
func up2date(rf *Raft, candidate *RequestVoteArgs) bool {

	lastEntry := rf.log[len(rf.log)-1]

	//If the logs have last entries with different terms,
	//then the log with the later term is more up2date(5.4.1)
	if candidate.LastLogTerm > lastEntry.Term {
		return true
	}

	//The candidate's log end with the same term,
	//then whichever log is longer is more up2date
	if candidate.LastLogTerm == lastEntry.Term &&
		candidate.LastLogIndex+1 >= len(rf.log) {
		return true
	}
	return false
}

//
// RequestVote RPC handler.
//
func (rf *Raft) RequestVote(candidate *RequestVoteArgs, reply *RequestVoteReply) {
	rf.Lock()
	defer rf.Unlock()
	DPrintf("RPC(Vote)   :%d <- %d -- Raft:%d(T:%2d)(S:%d)<-Raft:%d(T:%2d)\n",
		rf.me, candidate.CandidateID,
		rf.me, rf.currentTerm, rf.status, candidate.CandidateID, candidate.Term)
	// Your code here (2A, 2B).
	//LAB 2A
	//Reply false if candidate.Term < rf.currentTerm (5.1)
	term := rf.currentTerm
	if candidate.Term < term {
		reply.Term = term
		reply.VoteGranted = false
		return
	}

	//LAB 2A
	//If RPC request or response contains term T > currentTerm:
	//set currentTerm = T, convert to follower (5.1)
	//
	//if election timeout elapses without granting vote to candidate:
	//convert to candidate (5.2)
	if candidate.Term > term {
		rf.currentTerm = candidate.Term
		rf.votedFor = -1
		switch rf.status {
		case Candidate:
			c2f(rf)
		case Leader:
			l2f(rf)
		}
	}

	//LAB 2A
	//If votedFor is null or candidateId, and candidate’s log is at
	//least as up-to-date as receiver’s log, grant vote
	//Follower    votedFor == -1    && candidate.Term >= rf.currentTerm
	//Follower    votedFor != -1    && candidate.Term > rf.currentTerm
	//Candidate   votedFor == rf.me
	if (rf.votedFor == -1 ||
		rf.votedFor == candidate.CandidateID) &&
		up2date(rf, candidate) {
		resetTimer(rf.f2c, timeOut())
		rf.currentTerm = candidate.Term
		rf.votedFor = candidate.CandidateID
		reply.Term = term
		reply.VoteGranted = true
		DPrintf("RPC(VoteFor):%d VV %d -- Raft:%d(T:%2d)(S:%d) VoteFor Raft:%d(T:%2d)\n",
			rf.me, candidate.CandidateID,
			rf.me, rf.currentTerm, rf.status, candidate.CandidateID, candidate.Term)
	} else {
		reply.VoteGranted = false
	}
	return
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

//Append Entries request from leader
func appendEntries(rf *Raft, leader *RequestAppendArgs) {
	rf.Lock()
	defer rf.Unlock()
	return
}

//
// RequestAppend RPC handler.
//
func (rf *Raft) RequestAppend(leader *RequestAppendArgs, reply *RequestAppendReply) {
	rf.Lock()
	defer rf.Unlock()
	defer rf.persist()

	DPrintf("RPC(Append) :%d <- %d -- Raft:%d(T:%2d)(S:%d)<-Raft:%d(T:%2d)(Len:%d)\n",
		rf.me, leader.LeaderID,
		rf.me, rf.currentTerm, rf.status, leader.LeaderID, leader.Term, len(leader.Entries))
	//if len(leader.Entries) > 0 {
	//fmt.Printf("RPC(Append) :%d <- %d -- Raft:%d(T:%2d)(S:%d)<-Raft:%d(T:%2d)(Len:%d)(cmd:%v)(commitIndex:%d)\n",
	//	rf.me, leader.LeaderID,
	//	rf.me, rf.currentTerm, rf.status, leader.LeaderID, leader.Term, len(leader.Entries), leader.Entries[0], leader.LeaderCommit)
	//}
	//LAB 2A
	//Reply false if args.Term < rf.currentTerm (5.1)
	term := rf.currentTerm
	if leader.Term < term {
		reply.Term = term
		reply.Success = false
		return
	}

	//LAB 2A
	//If RPC request or response contains term T > currentTerm:
	//set currentTerm = T, convert to follower (5.1)
	if leader.Term > term {
		rf.currentTerm = leader.Term
		rf.votedFor = -1
		switch rf.status {
		case Candidate:
			c2f(rf)
		case Leader:
			l2f(rf)
		}
	}

	if leader.Term == term &&
		rf.status == Candidate {
		c2f(rf)
	}

	//Heartbeats - Append RPC with no log entry
	//Reset this follow's timer
	resetTimer(rf.f2c, timeOut())
	rf.currentTerm = leader.Term
	reply.Term = term
	reply.Success = true
	entry, ok := rf.log[leader.PrevLogIndex]
	//fmt.Printf("Raft.RequestAppend: preLogIdx:%d, preLogTerm:%d, entryTerm:%d, cmd:%v\n", leader.PrevLogIndex, leader.PrevLogTerm, entry.Term, leader.Entries[0])
	if !ok || entry.Term != leader.PrevLogTerm {

		idx := leader.PrevLogIndex
		logLen := len(rf.log)

		if ok {
			//5.3 Optimized protocol
			//one AppendEntries RPC will be required for each term with conflicting entries,
			//rather than one RPC per entry.

			//find the first index in confilict term and delete all the entries from this index
			for confilictIdx := 0; confilictIdx < logLen; confilictIdx++ {
				if rf.log[confilictIdx].Term == entry.Term {
					idx = confilictIdx
					break
				}
			}
		}

		for ; idx < logLen; idx++ {
			delete(rf.log, idx)
			//fmt.Printf("Raft.RequestAppend: Warning delete entries idx:%d, len:%d\n", idx, len(rf.log))
		}
		reply.Success = false
		return
	}

	//for k, v := range leader.Entries {
	//	fmt.Printf("Raft.RequestAppend: follower:%d appending, %d -> %d\n", rf.me, k, v)
	//}
	if reply.Success && len(leader.Entries) > 0 {
		for idx, e := range leader.Entries {
			rf.log[leader.PrevLogIndex+idx+1] = e
		}
	}
	//printLogs(rf)
	if leader.LeaderCommit > rf.commitIndex {
		f := math.Min(float64(leader.LeaderCommit), float64(len(rf.log)-1))
		rf.commitIndex = int(f)
		//fmt.Printf("Raft.RequestAppend: Follow:%d,Update commitIndex leaderCommit:%d, myCommit:%d\n", rf.me, leader.LeaderCommit, rf.commitIndex)
		//fmt.Printf("Raft.RequestAppend:commitIdx of Raft:%d update to %d\n", rf.me, rf.commitIndex)
	}
	return
}

//
// example code to send a RequestAppend RPC to a server.
//
func (rf *Raft) sendRequestAppend(server int, args *RequestAppendArgs, reply *RequestAppendReply) bool {
	ok := rf.peers[server].Call("Raft.RequestAppend", args, reply)
	return ok
}

//update the committed index for leader
//If there exists an N such that N > commitIndex, a majority
//of matchIndex[i] ≥ N, and log[N].term == currentTerm:
//set commitIndex = N (§5.3, §5.4).
func updateCommitIndex1(rf *Raft) {
	rf.Lock()
	defer rf.Unlock()
	cmtIdx := rf.commitIndex
	cmtNum := 0
	//for k1, v1 := range rf.matchIndex {
	//	DPrintf("updateCommitIndex: matchIndex -- k:%d -> v:%d\n", k1, v1)
	//}
	for {
		cmtIdx++
		cmtNum = 0
		DPrintf("updateCommitIndex:1 commitIdx:%d \n", cmtIdx)
		for _, v := range rf.matchIndex {
			if v >= cmtIdx {
				cmtNum++
			}
		}

		if cmtNum >= (len(rf.peers)/2 + 1) {
			//&& rf.log[cmtIdx].Term == rf.currentTerm{
			//fmt.Printf("updateCommitIndex: log[N].Term:%d , currentTerm:%d\n", rf.log[cmtIdx].Term, rf.currentTerm)
			continue
		} else {
			break
		}
	}

	if rf.commitIndex < cmtIdx-1 {
		rf.commitIndex = cmtIdx - 1
		//fmt.Printf("updateCommitIndex: commitIdx:%d \n", rf.commitIndex)
	}
}

func updateCommitIndex(rf *Raft) {
	rf.Lock()
	defer rf.Unlock()
	logLen := len(rf.log)
	term := rf.currentTerm
	cmtNum := 0
	cmtIdx := 0

	for idx := 0; idx < logLen; idx++ {
		if rf.log[idx].Term == term {
			cmtIdx = idx
			break
		}
	}

	if cmtIdx == 0 {
		return
	}

	for {
		cmtNum = 0
		//DPrintf("updateCommitIndex:1 commitIdx:%d \n", cmtIdx)
		for _, v := range rf.matchIndex {
			if v >= cmtIdx {
				cmtNum++
			}
		}

		if cmtNum >= (len(rf.peers)/2 + 1) {
			//fmt.Printf("updateCommitIndex: log[N].Term:%d , currentTerm:%d\n", rf.log[cmtIdx].Term, rf.currentTerm)
			if rf.log[cmtIdx].Term != rf.currentTerm {
				break
			} else {
				if cmtIdx > rf.commitIndex {
					DPrintf("leader:%d updateCommitIndex: update to %d\n", rf.me, cmtIdx)
					rf.commitIndex = cmtIdx
				}
			}
		} else {
			break
		}
		cmtIdx++
	}
}

//Send entries from leader to follower. If entries commited,
//apply the entires to the status machine
func sendEntries(rf *Raft) {
	rf.Lock()
	defer rf.Unlock()
	if rf.status != Leader {
		return
	}
	currentTerm := rf.currentTerm
	//me := rf.me
	logLen := len(rf.log)
	peersLen := len(rf.peers)

	//Prepare for all the replys
	replys := make([]RequestAppendReply, len(rf.peers))
	peers := rf.peers
	//mtx := &sync.Mutex{}

	//DPrintf("sendEntires: 1 Leader.commitIndex%d\n", rf.commitIndex)
	for idx := 0; idx < peersLen; idx++ {
		if idx == rf.me {
			continue
		}
		go func(idx int) {
			done := make(chan bool)
			taskState := false

		ReSent:
			prevIndex := rf.nextIndex[idx] - 1
			prevTerm := rf.log[prevIndex].Term
			entries := []Entry{}

			//mtx.Lock()
			for index := prevIndex + 1; index < logLen; index++ {
				entries = append(entries, rf.log[index])
			}
			//mtx.Unlock()

			retryArgs := &RequestAppendArgs{
				Term:         currentTerm,
				LeaderID:     rf.me,
				Entries:      entries,
				PrevLogIndex: prevIndex,
				PrevLogTerm:  prevTerm,
				LeaderCommit: rf.commitIndex,
			}

			DPrintf("sendEntires :%d -> %d -- Raft:%d(T:%2d)(S:%d)->Raft:%d\n",
				rf.me, idx,
				rf.me, rf.currentTerm, rf.status, idx)

			go func() {
				done <- peers[idx].Call("Raft.RequestAppend", retryArgs, &replys[idx])
			}()

			for {
				select {
				case taskState = <-done:
					//break
					goto Next
				case <-time.After(time.Duration(TimeFrom) * time.Millisecond):
					DPrintf("Warning..... Append resetTimer.........%d\n", rf.me)
					resetTimer(rf.f2c, timeOut())
				}
			}
		Next:

			//5.3 If followers crash or run slowly,
			//or if network packets are lost, the leader retries Append-
			//Entries RPCs indefinitely (even after it has responded to
			//the client) until all followers eventually store all log entries.
			if taskState == false {
				return
			}

			if replys[idx].Term > currentTerm {
				l2f(rf)
				return
			}

			if !replys[idx].Success {
				//fmt.Printf("sendEntires: Raft:%d Reply but not success, nextIndex:%d\n", idx, rf.nextIndex[idx])
				//5.3 Optimized protocol the leader can decrement nextIndex
				//to bypass all of the conflicting entries in that term

				//Find the first confilict index in that term
				for confilictIdx := 0; confilictIdx < logLen; confilictIdx++ {
					if rf.log[confilictIdx].Term == prevTerm {
						rf.nextIndex[idx] = confilictIdx
						break
					}
				}
				goto ReSent
			}

			if replys[idx].Success {
				rf.matchIndex[idx] = logLen - 1
				rf.nextIndex[idx] = logLen
				updateCommitIndex(rf)
				return
			}

		}(idx)
	} //end for
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.Lock()
	defer rf.Unlock()
	defer rf.persist()
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	if rf.status != Leader {
		return index, term, false
	}

	index = rf.nextIndex[rf.me]
	term = rf.currentTerm
	entry := Entry{Term: term, Command: command}
	//fmt.Printf("Raft.Start: Raft:%d, idx:%d, cmd:%d, term:%d\n", rf.me, index, entry.Command.(int), entry.Term)
	rf.log[index] = entry
	rf.matchIndex[rf.me] = index
	rf.nextIndex[rf.me] = index + 1
	//LAB 2B - 3. Issue AppendEnties RPC in parallel to each of the other servers
	//			  to replicate the entry

	//LAB 2B - 4. When themEntry have been saftly replicated,
	//			  the leader apply the entry to its state machine
	//go sendEntries(rf)

	//LAB 2B - 5. Return the results of that exection to the client
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.kill = true
	rf.status = Follower
	stopTimer(rf.f2c)
}

//
// Return the duration of timeout and avoid the split vote
// by randmon select timeout in the range of 150 to 300
// millisecond (5.2)
//
const (
	TimeFrom   = 150
	TimeTo     = 300
	HeartBeats = 87
	Leader     = 0
	Candidate  = 1
	Follower   = 2
)

func timeOut() time.Duration {
	rand.Seed(time.Now().UTC().UnixNano())
	randNum := rand.Intn(TimeTo-TimeFrom) + TimeFrom
	return time.Duration(randNum) * time.Millisecond
}

func electOnce(rf *Raft) {

	//rf.Lock()
	//defer rf.Unlock()
	//if rf.status != Candidate {
	//	return
	//}
	//rf.currentTerm++
	currentTerm := rf.currentTerm
	votedNum := 1
	mtx := &sync.Mutex{}
	//drain victory channel because victory will set to false many times
	//when a raftA is candidate and it get reject vote from raftB with bigger term
	//at the same time a leader(raftC) have been selected and send heartbeat to raftA

	//Issues RequestVote RPCs in parallel to each of
	//the other servers in the cluster.(5.2)
	args := &RequestVoteArgs{}
	args.Term = rf.currentTerm
	args.CandidateID = rf.me
	args.LastLogTerm = rf.log[len(rf.log)-1].Term
	args.LastLogIndex = len(rf.log) - 1

	replys := make([]RequestVoteReply, len(rf.peers))
	peers := rf.peers
	//DPrintf("Raft:%d(term:%d)(status:%d)...F2C\n", rf.me, rf.currentTerm, rf.status)

	for idx := 0; idx < len(peers); idx++ {
		if idx == rf.me {
			continue
		}
		go func(idx int) {
			done := make(chan bool)
			taskState := false

			if rf.status != Candidate {
				return
			}
			DPrintf("ElectOnce   :%d -> %d -- Raft:%d(T:%2d)(S:%d)->Raft:%d\n",
				rf.me, idx,
				rf.me, rf.currentTerm, rf.status, idx)

			go func() {
				done <- peers[idx].Call("Raft.RequestVote", args, &replys[idx])
			}()

			for {
				select {
				case taskState = <-done:
					//break
					goto Next
				case <-time.After(time.Duration(TimeFrom) * time.Millisecond):
					DPrintf("Warning..... Vote resetTimer.........%d\n", rf.me)
					resetTimer(rf.f2c, timeOut())
				}
			}
		Next:

			//Servers retry RPCs if they do not receive a response
			//in a timely manner(5.1 last line).
			//This means retry after the rpc timeout interval ,
			//so no need to implement your own timeouts around Call

			//If a follower of or candidate creashed, the future RequestVote
			//and AppendEntries PRC sent to it will fail. Raft handles these
			//failurs by retrying indefinitely.(5.5)
			if taskState == false {
				//fmt.Printf("Raft.Election:RPC error - %v send to %v\n", me, idx)
				return
			}

			if replys[idx].Term > currentTerm {
				c2f(rf)
				return
			}

			if replys[idx].VoteGranted == true {
				mtx.Lock()
				votedNum++
				mtx.Unlock()
				if votedNum >= (len(peers)/2 + 1) {
					c2l(rf)
					return
				}
			}
		}(idx)

	} //end for
}

//
//After duration expires, the leader election startup
//
func f2c(rf *Raft) {

	rf.Lock()
	defer rf.Unlock()
	DPrintf("F2C........  %d(T:%2d)(S:%d)\n", rf.me, rf.currentTerm, rf.status)
	//Stop the timer firstly
	//To begin an election, a follower increments its current
	//term and transitions to candidate state. It then votes for
	//itself (5.2)
	if rf.status == Leader {
		return
	}
	rf.status = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	resetTimer(rf.f2c, timeOut())
	electOnce(rf)
	//drain c2f channel because victory will set to false many times
	//when a raftA is candidate and it get reject vote from raftB with bigger term
	//at the same time a leader(raftC) have been selected and send heartbeat to raftA
	//drainChan(rf.c2l)
	//drainChan(rf.c2f)
}

//
//Reset timer
//
func stopTimer(t *time.Timer) {
	if !t.Stop() {
		select {
		case <-t.C:
			//fmt.Println("resetTimer: What happend??")
			//fmt.Println("resetTimer: This means timeout happend but no electioin trigged!")
		default:
		}
	}
}

func resetTimer(t *time.Timer, d time.Duration) {
	stopTimer(t)
	t.Reset(d)
}

func c2l(rf *Raft) {
	rf.Lock()
	defer rf.Unlock()

	if rf.status == Leader {
		return
	}
	DPrintf("C2L........  %d(T:%2d)(S:%d)\n", rf.me, rf.currentTerm, rf.status)
	//fmt.Printf("C2L........  %d(T:%2d)(S:%d)\n", rf.me, rf.currentTerm, rf.status)
	//Stop the timeout timer
	stopTimer(rf.f2c)
	//candidate to leader
	rf.status = Leader
	//rf.votedFor = -1
	//Send the heart beat to all the others
	heartbeatD := time.Duration(HeartBeats) * time.Millisecond
	rf.heartbeatT = time.NewTimer(time.Duration(0))
	go func() {
		for {
			if rf.kill {
				return
			}
			select {
			case <-rf.heartbeatT.C:
				resetTimer(rf.heartbeatT, heartbeatD)
				//gongzhe
				//go sendEntries(rf)
				sendEntries(rf)
			}

		}
	}()
	//fmt.Println("C2L          ............. ", rf.me)
	for k, _ := range rf.nextIndex {
		rf.nextIndex[k] = len(rf.log)
	}
}

func c2f(rf *Raft) {
	//candidate to follower
	if rf.status == Follower {
		return
	}
	DPrintf("C2F........  %d(T:%2d)(S:%d)\n", rf.me, rf.currentTerm, rf.status)
	rf.status = Follower
	//rf.currentTerm = rf.currentTerm - 1
	rf.votedFor = -1
	resetTimer(rf.f2c, timeOut())
	//fmt.Println("C2F          ............. ", rf.me)
}

func l2f(rf *Raft) {
	//Leader to follower
	if rf.status == Follower {
		return
	}
	DPrintf("L2F........  %d(T:%2d)(S:%d)\n", rf.me, rf.currentTerm, rf.status)
	//fmt.Printf("L2F........  %d(T:%2d)(S:%d)\n", rf.me, rf.currentTerm, rf.status)
	rf.status = Follower
	rf.votedFor = -1
	//stop the heartbeat
	stopTimer(rf.heartbeatT)
	//reset timeout timer
	resetTimer(rf.f2c, timeOut())
	//fmt.Println("L2F          ............. ", rf.me)
}

func drainChan(c chan bool) {
	for {
		select {
		case <-c:
			fmt.Println("drain the chan .............")
			continue
		default:
			return
		}
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.kill = false
	rf.currentTerm = 0
	rf.status = Follower
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.votedFor = -1
	//array := [3]int{149, 145, 140}
	//array := [3]int{149, 149, 149}
	//rf.f2c = time.NewTimer(time.Duration(array[me]) * time.Millisecond)
	rf.f2c = time.NewTimer(timeOut())
	rf.c2l = make(chan bool)
	rf.c2f = make(chan bool)
	rf.log = make(map[int]Entry)
	//Lab 2B
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	//rf.entriesChan = make(chan ApplyMsg, 100)
	//Lab 2B init the rf.log[0].Term = 0
	entry := Entry{Term: 0}
	rf.log[0] = entry

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// Your initialization code here (2A, 2B, 2C).
	//LAB 2A
	go func() {
		//Kick off leader election periodically
		for {
			if rf.kill {
				return
			}
			select {
			case <-rf.f2c.C:
				f2c(rf)
			default:
				if index := rf.lastApplied; index < rf.commitIndex {
					//fmt.Printf("Applying: Raft:%d: lastApplied:%d, commitIdx:%d\n", rf.me, rf.lastApplied, rf.commitIndex)
					applyCh <- ApplyMsg{Index: index + 1, Command: rf.log[index+1].Command}
					rf.lastApplied++
					//printLogs(rf)
				}
			}
		}
	}()

	return rf
}

func printLogs(rf *Raft) {
	for key, e := range rf.log {
		fmt.Printf("PrintLogs: Raft:%d, %d -> %v\n",
			rf.me, key, e)
	}
}
