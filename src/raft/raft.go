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
	//	"bytes"

	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

var HEARTBEAT_MS int = 150
var ELECTION_TIMEOUT_MS_MIN int = 600
var ELECTION_TIMEOUT_MS_MAX int = 750
var RETRY_APPEND_ENTRIES int = 0
var FOLLOWER int = 0
var CANDIDATE int = 1
var LEADER int = 2
var PRINT_LOGS bool = false

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	LogIndex int
	Term     int
	Value    interface{}
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
	currentTerm int
	votedFor    int
	log         []LogEntry
	commitIndex int
	lastApplied int
	role        int
	applyCh     chan ApplyMsg

	// Leader state
	nextIndex             []int
	matchIndex            []int
	lastSentAppendEntries []time.Time

	// Follower state
	lastHeartbeat time.Time
	lastVoteGrant time.Time

	// Candidate state
	hasVote []bool
}

func (rf *Raft) print(topic string, str string, a ...interface{}) {
	if !PRINT_LOGS {
		return
	}

	roleStr := ""
	// switch rf.role {
	// case LEADER:
	// 	roleStr = "Leader"
	// case FOLLOWER:
	// 	roleStr = "Follower"
	// case CANDIDATE:
	// 	roleStr = "Candidate"
	// }

	now := time.Now()
	timeStr := now.Format("15:04:05.000")
	// s := fmt.Sprintf("[%v] %v - %v (%v, Term %v) - %v\n", topic, timeStr, rf.me, roleStr, rf.currentTerm, str)
	s := fmt.Sprintf("[%v] %v - %v (%v, Term %v) - %v\n", topic, timeStr, rf.me, roleStr, -1, str)
	fmt.Printf(s, a...)
}

// If discovering new term or generally entering new term as follower, run this
func (rf *Raft) updateTermAndReset(newTerm int) {
	rf.print("STCH", "Found new term %v, going back to follower", newTerm)
	rf.currentTerm = newTerm
	rf.votedFor = -1
	rf.role = FOLLOWER
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	rf.print("LOCK", "Trying to lock in GetState")
	rf.mu.Lock()
	rf.print("LOCK", "succeeded to lock in GetState")
	term = rf.currentTerm
	isleader = rf.role == LEADER
	rf.print("LOCK", "finished lock in GetState")
	rf.mu.Unlock()
	// Your code here (2A).
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
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.print("VOTE", "Received RequestVote from %v with term %v", args.CandidateId, args.Term)
	defer rf.print("VOTE", "Voted %v", reply)
	// Your code here (2A, 2B).
	rf.print("LOCK", "Trying to lock in RequestVote")
	rf.mu.Lock()
	rf.print("LOCK", "succeeded to lock in RequestVote")
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	// If candidate behind this one
	if args.Term < rf.currentTerm {
		rf.print("LOCK", "finished lock in RequestVote")
		rf.mu.Unlock()
		return
	}

	// If behind, update
	if args.Term > rf.currentTerm {
		rf.updateTermAndReset(args.Term)
		reply.Term = rf.currentTerm
	}

	// Check if at least as up to date
	lastTerm := -1
	lastLogIndex := -1
	if len(rf.log) > 0 {
		last := rf.log[len(rf.log)-1]
		lastTerm = last.Term
		lastLogIndex = last.LogIndex
	}

	if args.LastLogTerm < lastTerm {
		rf.print("LOCK", "finished lock in RequestVote")
		rf.mu.Unlock()
		return
	}
	if args.LastLogTerm == lastTerm && args.LastLogIndex < lastLogIndex {
		rf.print("LOCK", "finished lock in RequestVote")
		rf.mu.Unlock()
		return
	}

	// Grant vote if not voted
	if rf.votedFor == -1 {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.lastVoteGrant = time.Now()
	}
	rf.print("LOCK", "finished lock in RequestVote")
	rf.mu.Unlock()
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

func majoritySatisfiesF[T interface{}](arr []T, f func(t T) bool) bool {
	majority := len(arr)/2 + 1
	count := 0
	for i := range arr {
		if f(arr[i]) {
			count++
			if count >= majority {
				return true
			}
		}
	}
	return false
}

func (rf *Raft) sendAppendEntries(server int, initialEntries []LogEntry) {
	rf.print("LOCK", "Trying to lock in sendAppendEntries")
	rf.mu.Lock()
	rf.print("LOCK", "succeeded to lock in sendAppendEntries")
	prevLogIndex, prevLogTerm := -1, -1
	if len(rf.log) > 0 {
		prevLogIndex = rf.nextIndex[server] - 1
		prevLogTerm = rf.log[prevLogIndex-rf.log[0].LogIndex].Term
	}
	entries := initialEntries

	// Run until successful
	for rf.killed() == false {
		// Or no longer leader
		if rf.role != LEADER {
			return
		}

		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}
		reply := AppendEntriesReply{}
		rf.print("HTBT", "Sending appendEntries to %v (%v)", server, args)
		rf.print("LOCK", "finished lock in sendAppendEntries")
		rf.mu.Unlock()
		ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)

		if !ok {
			rf.print("HTBT", "failed to contact server %v for AppendEntries %v", server, args)
			// Wait (don't send more heartbeats) and retry
			time.Sleep(time.Duration(RETRY_APPEND_ENTRIES) * time.Millisecond)
			break
		}
		rf.print("LOCK", "Trying to lock in sendAppendEntries 2")
		rf.mu.Lock()
		rf.print("LOCK", "succeeded to lock in sendAppendEntries 2")
		if rf.role != LEADER {
			rf.print("LOCK", "finished lock in sendAppendEntries 2")
			rf.mu.Unlock()
			return
		}

		// update next/matchIndex if successful
		if reply.Success {
			newLogIndex := len(entries) + prevLogTerm
			rf.nextIndex[server] = newLogIndex + 1
			rf.matchIndex[server] = newLogIndex
			// Check if replicated on majority of servers
			if majoritySatisfiesF(rf.matchIndex, func(i int) bool { return i >= newLogIndex }) {
				if newLogIndex > rf.commitIndex {
					// Apply up thru committed
					oldCommitIndex := rf.commitIndex
					startIndex := -1
					if len(rf.log) > 0 {
						startIndex = len(rf.log) - 1 - (rf.log[len(rf.log)-1].LogIndex - oldCommitIndex)
					}
					rf.commitIndex = newLogIndex
					for i := 1; i <= newLogIndex-oldCommitIndex; i++ {
						rf.applyCh <- ApplyMsg{CommandValid: true, Command: rf.log[startIndex+i].Value, CommandIndex: oldCommitIndex + i}
					}
				}
			}
			rf.print("LOCK", "finished lock in sendAppendEntries 2")
			rf.mu.Unlock()
			break
		}

		// Check if out of date (in which case, stop trying)
		if reply.Term > rf.currentTerm {
			rf.updateTermAndReset(reply.Term)
			rf.print("LOCK", "finished lock in sendAppendEntries 2")
			rf.mu.Unlock()
			break
		}

		// We know there was a failure
		rf.nextIndex[server]--
		entries = append([]LogEntry{rf.log[rf.nextIndex[server]]}, entries...)
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.print("HTBT", "Receiving appendEntries from %v", args.LeaderId)
	rf.print("LOCK", "Trying to lock in AppendEntries")
	rf.mu.Lock()
	rf.print("LOCK", "succeeded to lock in AppendEntries")
	// Check if commitIndex is greater than this one. If so, apply up to there
	if args.LeaderCommit > rf.commitIndex {
		startIndex := 0
		if len(rf.log) > 0 {
			startIndex = len(rf.log) - (rf.log[len(rf.log)-1].LogIndex - rf.commitIndex)
		}
		for i := startIndex; i <= args.LeaderCommit; i++ {
			rf.applyCh <- ApplyMsg{CommandValid: true, Command: rf.log[i].Value, CommandIndex: rf.log[i].LogIndex}
		}
		rf.commitIndex = args.LeaderCommit
	}

	rf.lastHeartbeat = time.Now()
	reply.Success = false
	reply.Term = rf.currentTerm
	if args.Term > rf.currentTerm {
		rf.updateTermAndReset(args.Term)
		reply.Term = rf.currentTerm
	}

	// Reply false if term < current term
	if args.Term < rf.currentTerm {
		rf.print("LOCK", "finished lock in AppendEntries")
		rf.mu.Unlock()
		return
	}
	// Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
	if len(rf.log) > 0 {
		idx := args.PrevLogIndex - rf.log[0].LogIndex
		if len(rf.log) >= idx && rf.log[idx].Term != args.PrevLogTerm {
			rf.print("LOCK", "finished lock in AppendEntries")
			rf.mu.Unlock()
			return
		}
	}

	for i := range args.Entries {
		idx := i + args.PrevLogIndex + 1
		// Check if conflict
		if len(rf.log) >= idx {
			if rf.log[idx].Term != args.Entries[i].Term {
				rf.log = rf.log[:idx]
			}
		}

		// Append log
		if len(rf.log) < idx {
			rf.log = append(rf.log, args.Entries[i:]...)
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		newCommit := args.LeaderCommit
		newestLogIndex := -1
		if len(rf.log) > 0 {
			newestLogIndex = rf.log[len(rf.log)-1].LogIndex
		}
		if newestLogIndex > newCommit {
			newCommit = newestLogIndex
		}
		rf.commitIndex = newCommit
	}
	reply.Success = true
	rf.print("LOCK", "finished lock in AppendEntries")
	rf.mu.Unlock()
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
	rf.print("LOCK", "Trying to lock in Start")
	rf.mu.Lock()
	rf.print("LOCK", "succeeded to lock in Start")
	index := 0
	if len(rf.log) > 0 {
		index = rf.log[len(rf.log)-1].LogIndex + 1
	}
	term := rf.currentTerm
	isLeader := rf.role == LEADER
	// If not leader, return false
	if !isLeader {
		rf.print("LOCK", "finished lock in Start")
		rf.mu.Unlock()
		return index, term, isLeader
	}

	// Append to log
	newEntry := LogEntry{LogIndex: index, Term: term, Value: command}
	rf.log = append(rf.log, newEntry)
	// Tell all other servers to append
	for i := range rf.peers {
		go rf.sendAppendEntries(i, []LogEntry{newEntry})
	}

	rf.print("LOCK", "finished lock in Start")
	rf.mu.Unlock()
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
	rf.print("KILL", "killed")
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) heartbeatOne(server int) {
	// Stop if not leader
	rf.print("LOCK", "Trying to lock in heartbeatOne")
	rf.mu.Lock()
	rf.print("LOCK", "succeeded to lock in heartbeatOne")
	if rf.role != LEADER {
		rf.print("LOCK", "finished lock in heartbeatOne")
		rf.mu.Unlock()
		return
	}

	now := time.Now()
	if now.Sub(rf.lastSentAppendEntries[server]).Milliseconds() > int64(HEARTBEAT_MS) {
		// time to send heartbeat
		go rf.sendAppendEntries(server, make([]LogEntry, 0))
		rf.lastSentAppendEntries[server] = now
	}

	rf.print("LOCK", "finished lock in heartbeatOne")
	rf.mu.Unlock()
	time.Sleep(time.Duration(HEARTBEAT_MS) * time.Millisecond)
	if rf.killed() == false {
		rf.heartbeatOne(server)
	}
}

// Sends heartbeats automatically if leader
func (rf *Raft) heartbeat() {
	for i := range rf.lastSentAppendEntries {
		if i != rf.me {
			go rf.heartbeatOne(i)
		}
	}
}

func (rf *Raft) requestOneVote(server int) {
	rf.print("LOCK", "Trying to lock in requestOneVote")
	rf.mu.Lock()
	rf.print("LOCK", "succeeded to lock in requestOneVote")
	// Don't do anything if no longer candidate
	if rf.role != CANDIDATE {
		rf.print("LOCK", "finished lock in requestOneVote")
		rf.mu.Unlock()
		return
	}
	rf.print("CNDT", "Sending RequestVote to %v", server)

	lastLogIndex := -1
	lastLogTerm := -1
	if len(rf.log) > 0 {
		lastLog := rf.log[len(rf.log)-1]
		lastLogIndex = lastLog.LogIndex
		lastLogTerm = lastLog.Term
	}
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	reply := RequestVoteReply{}

	// Do not hold lock while waiting
	rf.print("LOCK", "finished lock in requestOneVote")
	rf.mu.Unlock()
	rf.sendRequestVote(server, &args, &reply)
	rf.print("LOCK", "Trying to lock in requestOneVote #2")
	rf.mu.Lock()
	rf.print("LOCK", "succeeded to lock in requestOneVote #2")
	if rf.role != CANDIDATE {
		rf.print("LOCK", "finished lock in requestOneVote #2")
		rf.mu.Unlock()
		return
	}

	// If term is greater, switch back to follower
	if reply.Term > rf.currentTerm {
		rf.updateTermAndReset(reply.Term)
	}
	rf.hasVote[server] = reply.VoteGranted

	// Check if majority
	majority := len(rf.peers)/2 + 1
	numVotes := 0
	for i := range rf.hasVote {
		if rf.hasVote[i] {
			numVotes++
		}
	}
	if numVotes >= majority {
		rf.role = LEADER
		rf.print("STCH", "Becoming leader! term: %v, votes: %v", rf.currentTerm, rf.hasVote)
		rf.print("LOCK", "finished lock in requestOneVote #2")
		rf.mu.Unlock()
		rf.heartbeat()
	} else {
		rf.print("LOCK", "finished lock in requestOneVote #2")
		rf.mu.Unlock()
	}
}

func (rf *Raft) requestVotes() {
	for i := range rf.peers {
		if i != rf.me {
			go rf.requestOneVote(i)
		}
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		delay := rand.Intn(ELECTION_TIMEOUT_MS_MAX-ELECTION_TIMEOUT_MS_MIN+1) + ELECTION_TIMEOUT_MS_MIN

		t := time.Now()
		rf.print("LOCK", "Trying to lock in ticker")
		rf.mu.Lock()
		rf.print("LOCK", "succeeded to lock in ticker")
		timeSinceLastHeartbeat := t.Sub(rf.lastHeartbeat)
		timeSinceLastVoteGrant := t.Sub(rf.lastVoteGrant)
		// Hasn't received heartbeat -> time to request votes
		if timeSinceLastHeartbeat.Milliseconds() > int64(delay) && timeSinceLastVoteGrant.Milliseconds() > int64(delay) { // Lock so weird things don't happen
			if rf.role == FOLLOWER || rf.role == CANDIDATE {
				rf.role = CANDIDATE
				rf.currentTerm++
				rf.print("STCH", "becoming candidate!")

				// Vote for self
				rf.votedFor = rf.me
				for i := range rf.hasVote {
					rf.hasVote[i] = i == rf.me
				}
				rf.print("LOCK", "finished lock in ticker")
				rf.mu.Unlock()
				// Request votes
				rf.requestVotes()
			} else {
				rf.print("LOCK", "finished lock in ticker")
				rf.mu.Unlock()
			}
		} else {
			rf.print("LOCK", "finished lock in ticker")
			rf.mu.Unlock()
		}

		time.Sleep(time.Duration(delay) * time.Millisecond)
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
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.role = FOLLOWER
	rf.applyCh = applyCh

	// Leader state
	// TODO: This will have to be initialized to non-0 later
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.lastSentAppendEntries = make([]time.Time, len(peers))
	now := time.Now()
	for i := range rf.lastSentAppendEntries {
		rf.lastSentAppendEntries[i] = now
	}

	// Follower state
	rf.lastHeartbeat = time.Now()
	rf.lastVoteGrant = time.Now()

	// Candidate state
	rf.hasVote = make([]bool, len(peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.print("STRT", "started")
	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
