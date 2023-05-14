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

	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

var HEARTBEAT_MS int = 150
var ELECTION_TIMEOUT_MS_MIN int = 600
var ELECTION_TIMEOUT_MS_MAX int = 750
var RETRY_APPEND_ENTRIES int = 0
var FOLLOWER int = 0
var CANDIDATE int = 1
var LEADER int = 2
var PRINT_LOGS bool = true
var PRINT_LOCKS bool = true
var PRINT_HTBT bool = false
var PRINT_DBUG bool = true

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

type ToCommit struct {
	entries 	[]LogEntry
	lastIndex	int
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
	currentTerm 			int
	votedFor    			int
	log         			[]LogEntry
	commitIndex 			int
	lastApplied 			int // The last log index which has been sent to commitCh and "applied"
	lastAppliedCompleted 	int // The last log index that was actually sent to applyCh
	role        			int
	applyCh     			chan ApplyMsg
	commitCh 				chan ToCommit

	// Snapshot state
	lastSnapshot      []byte
	lastIncludedTerm  int
	lastIncludedIndex int

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
	if !PRINT_LOCKS && topic == "LOCK" {
		return
	}
	if !PRINT_DBUG && topic == "DBUG" {
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
	rf.persist(nil)
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
func (rf *Raft) persist(snapshot []byte) {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data, snapshot)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		panic("Failed to decode!")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
	}
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
	rf.print("LOCK", "Trying to lock in Snapshot")
	rf.mu.Lock()
	rf.print("LOCK", "succeeded to lock in Snapshot")

	indexInLog := index - rf.log[0].LogIndex
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = rf.log[indexInLog].Term
	rf.lastSnapshot = snapshot
	rf.log = rf.log[indexInLog+1:]
	rf.persist(snapshot)
	rf.applyCh <- ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		Snapshot:      snapshot,
		SnapshotTerm:  rf.lastIncludedTerm,
		SnapshotIndex: rf.lastIncludedIndex,
	}

	rf.mu.Unlock()
	rf.print("LOCK", "finished lock in Snapshot")
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
	Term                  int
	Success               bool
	FirstIndexOfErrorTerm int
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.print("TRCE", "Received InstallSnapshot from %v", args.LeaderId)
	rf.print("LOCK", "Trying to lock in InstallSnapshot")
	rf.mu.Lock()
	rf.print("LOCK", "succeeded to lock in InstallSnapshot")

	reply.Term = rf.currentTerm
	// Reply immediately if term < currentTerm, or if old data
	if args.Term < rf.currentTerm || args.LastIncludedIndex <= rf.lastApplied {
		rf.print("LOCK", "finished lock in InstallSnapshot")
		rf.mu.Unlock()
		return
	}

	// Check if term needs to be updated
	if args.Term > rf.currentTerm {
		rf.updateTermAndReset(args.Term)
		reply.Term = args.Term
	}

	// If log does not contain the index or does not match
	if len(rf.log) == 0 ||
		args.LastIncludedIndex-rf.log[0].LogIndex >= len(rf.log) ||
		rf.log[args.LastIncludedIndex-rf.log[0].LogIndex].Term != rf.lastIncludedTerm {
		rf.log = []LogEntry{}
	} else {
		// Log contains a matching entry
		indexInLog := rf.log[0].LogIndex - args.LastIncludedIndex
		rf.log = rf.log[indexInLog+1:]
	}
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.lastSnapshot = args.Data
	rf.persist(args.Data)
	rf.applyCh <- ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  rf.lastIncludedTerm,
		SnapshotIndex: rf.lastIncludedIndex,
	}

	rf.print("LOCK", "finished lock in InstallSnapshot")
	rf.mu.Unlock()
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
		rf.persist(nil)
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

func (rf *Raft) sendInstallSnapshot(server int) {
	rf.print("LOCK", "Trying to lock in sendInstallSnapshot")
	rf.mu.Lock()
	rf.print("LOCK", "succeeded to lock in sendInstallSnapshot")

	// Run while leader and until successful
	for rf.killed() == false && rf.role == LEADER {
		args := InstallSnapshotArgs{
			Term:              rf.currentTerm,
			LeaderId:          rf.me,
			LastIncludedIndex: rf.lastIncludedIndex,
			LastIncludedTerm:  rf.lastIncludedTerm,
			Data:              rf.lastSnapshot,
		}
		reply := InstallSnapshotReply{}
		rf.print("LOCK", "finished lock in sendInstallSnapshot")
		rf.mu.Unlock()
		ok := rf.peers[server].Call("Raft.InstallSnapshot", &args, &reply)

		// Check for failure
		if !ok {
			rf.print("HTBT", "failed to contact server %v for InstallSnapshot %v", server, args)
			time.Sleep(time.Duration(RETRY_APPEND_ENTRIES) * time.Millisecond)
			rf.print("LOCK", "Trying to re-lock in sendInstallSnapshot")
			rf.mu.Lock()
			continue
		}
		rf.print("LOCK", "Trying to lock in sendInstallSnapshot 2")
		rf.mu.Lock()
		rf.print("LOCK", "Succeeded to lock in sendInstallSnapshot 2")
		// Check if behind in term
		if reply.Term > rf.currentTerm {
			rf.updateTermAndReset(reply.Term)
			continue
		}

		// Success
		break
	}

	rf.print("LOCK", "finished lock in sendInstallSnapshot")
	rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntries(server int) {
	rf.print("LOCK", "Trying to lock in sendAppendEntries")
	rf.mu.Lock()
	rf.print("LOCK", "succeeded to lock in sendAppendEntries")
	prevLogIndex, prevLogTerm := 0, -1
	if len(rf.log) > 0 {
		prevLogIndex = rf.nextIndex[server] - 1
		if prevLogIndex != 0 {
			rf.print("DBUG", "Getting prevLogTerm for server %v. log %v, prevLogIndex %v", server, rf.log, prevLogIndex)
			prevLogTerm = rf.log[prevLogIndex-rf.log[0].LogIndex].Term
		}
	}
	entries := []LogEntry{}

	// Run until successful
	for rf.killed() == false {
		// Or no longer leader
		if rf.role != LEADER {
			rf.print("LOCK", "finished lock in sendAppendEntries")
			rf.mu.Unlock()
			return
		}

		// Catch up server if necessary
		if len(rf.log) > 0 || rf.lastIncludedIndex > 0 {
			// Check if we need to install snapshot
			if prevLogIndex < rf.lastIncludedIndex || (prevLogIndex+1)-rf.log[0].LogIndex < 0 {
				// Send snapshot
				rf.print("LOCK", "finished lock in sendAppendEntries")
				rf.mu.Unlock()
				go rf.sendInstallSnapshot(server)
				break
			} else {
				// Just some entries to add
				entries = rf.log[(prevLogIndex+1)-rf.log[0].LogIndex:]
			}
		} else {
			entries = []LogEntry{}
		}
		sendEntries := make([]LogEntry, len(entries))
		for i := range entries {
			sendEntries[i] = entries[i]
		}

		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      sendEntries,
			LeaderCommit: rf.commitIndex,
		}
		reply := AppendEntriesReply{}
		if len(entries) != 0 || PRINT_HTBT {
			rf.print("HTBT", "Sending appendEntries to %v (%v)", server, args)
		}
		rf.print("LOCK", "finished lock in sendAppendEntries")
		rf.mu.Unlock()
		ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)

		if !ok {
			rf.print("HTBT", "failed to contact server %v for AppendEntries %v", server, args)
			// Wait (don't send more heartbeats) and retry
			time.Sleep(time.Duration(RETRY_APPEND_ENTRIES) * time.Millisecond)
			rf.print("LOCK", "Trying to re-lock in sendAppendEntries")
			rf.mu.Lock()
			continue
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
			newLogIndex := 0
			if len(entries) > 0 {
				newLogIndex = entries[len(entries)-1].LogIndex
			} else if len(rf.log) > 0 {
				if rf.matchIndex[server] > 0 {
					newLogIndex = rf.log[rf.matchIndex[server]-rf.log[0].LogIndex].LogIndex
				}
			}

			if newLogIndex+1 > rf.nextIndex[server] {
				rf.nextIndex[server] = newLogIndex + 1
			}
			if newLogIndex > rf.matchIndex[server] {
				rf.matchIndex[server] = newLogIndex
			}
			rf.print("TRCE", "Updating server %v nextIndex to %v, matchIndex to %v (prevLogIndex %v, newLogIndex %v, commitIndex %v)", server, rf.nextIndex[server], rf.matchIndex[server], prevLogIndex, newLogIndex, rf.commitIndex)
			// Check if replicated on majority of servers
			if majoritySatisfiesF(rf.matchIndex, func(i int) bool { return i >= newLogIndex }) {
				if newLogIndex > rf.commitIndex {
					rf.print("CMMT", "Committing up through %v", newLogIndex)
					// Apply up thru committed
					oldCommitIndex := rf.commitIndex
					startIndex := -1
					if len(rf.log) > 0 {
						startIndex = len(rf.log) - 1 - (rf.log[len(rf.log)-1].LogIndex - oldCommitIndex)
					}
					rf.commitIndex = newLogIndex
					rf.lastApplied = newLogIndex
					rf.print("DBUG", "Attempting to commit. prevLogIndex %v, startIndex %v, newLogIndex %v, oldCommitIndex %v. Log is %v long", prevLogIndex, startIndex, newLogIndex, oldCommitIndex, len(rf.log))
					rf.commitCh <- ToCommit{
						entries: rf.log[startIndex+1:startIndex+newLogIndex-oldCommitIndex+1],
						lastIndex: newLogIndex,
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

		// We know there was a failure. Check if we can skip some entries
		rf.print("DBUG", "reply %v", reply)
		if reply.FirstIndexOfErrorTerm != 0 {
			rf.nextIndex[server] = reply.FirstIndexOfErrorTerm
		} else {
			rf.nextIndex[server]--
		}
		prevLogIndex = rf.nextIndex[server] - 1
		prevLogTerm = rf.log[prevLogIndex-rf.log[0].LogIndex].Term
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if len(args.Entries) != 0 || PRINT_HTBT {
		rf.print("HTBT", "Receiving appendEntries from %v - %v.", args.LeaderId, args.Entries)
	}
	rf.print("LOCK", "Trying to lock in AppendEntries")
	rf.mu.Lock()
	rf.print("LOCK", "succeeded to lock in AppendEntries")
	if len(args.Entries) != 0 {
		rf.print("DBUG", "In appendEntries with non-empty message. Log is currently %v", rf.log)
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
		if args.PrevLogIndex > 0 && (idx < 0 || idx >= len(rf.log) || rf.log[idx].Term != args.PrevLogTerm) {
			if idx >= 0 && idx < len(rf.log) {
				// Conflicting entry
				reply.FirstIndexOfErrorTerm = 1
				conflictingTerm := rf.log[idx].Term
				searchIdx := idx
				for searchIdx >= 0 {
					if rf.log[searchIdx].Term != conflictingTerm {
						reply.FirstIndexOfErrorTerm = rf.log[searchIdx].LogIndex
						break
					}
					searchIdx--
				}
			}
			rf.print("LOCK", "finished lock in AppendEntries")
			rf.mu.Unlock()
			return
		}
	} else if args.PrevLogIndex > 0 {
		rf.print("LOCK", "finished lock in AppendEntries")
		rf.mu.Unlock()
		return
	}

	if len(args.Entries) > 0 {
		rf.print("DBUG", "Going to try appending entries. log %v", rf.log)
	}
	for i := range args.Entries {
		idx := 0
		if len(rf.log) > 0 {
			idx = args.PrevLogIndex - rf.log[0].LogIndex + i + 1
		}
		// Check if conflict
		if idx < len(rf.log) {
			if rf.log[idx].Term != args.Entries[i].Term {
				rf.print("LOGS", "Chopping off log entries %v thru %v", idx, len(rf.log)-1)
				rf.log = rf.log[:idx]
			}
		}

		// Append log
		if idx >= len(rf.log) {
			rf.print("LOGS", "Appending %v to log (starting at spot %v)", args.Entries, len(rf.log))
			rf.log = rf.log[:idx]
			rf.log = append(rf.log, args.Entries[i:]...)
		}
	}
	rf.persist(nil)

	// Check if commitIndex is greater than this one. If so, apply up to there
	if args.LeaderCommit > rf.commitIndex {
		newCommit := args.LeaderCommit
		newestLogIndex := 0
		if len(rf.log) > 0 {
			newestLogIndex = rf.log[len(rf.log)-1].LogIndex
		}
		if newestLogIndex < newCommit {
			newCommit = newestLogIndex
		}
		startIndex := 0
		if len(rf.log) > 0 {
			startIndex = len(rf.log) - (rf.log[len(rf.log)-1].LogIndex - rf.commitIndex)
		}
		rf.commitIndex = newCommit
		rf.lastApplied = newCommit
		rf.print("DBUG", "newestLogIndex %v, leaderCommit %v, startIndex %v, log %v", newestLogIndex, args.LeaderCommit, startIndex, rf.log)
		
		lastIndex := newCommit - rf.log[0].logIndex
		if lastIndex > len(rf.log) - 1 {
			lastIndex = len(rf.log) - 1
		}

		rf.commitCh <- ToCommit{
			entries: rf.log[startIndex:lastIndex+1],
			lastIndex: newLogIndex,
		}
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
	rf.print("CLNT", "Received command %v", command)
	rf.print("LOCK", "Trying to lock in Start")
	rf.mu.Lock()
	rf.print("LOCK", "succeeded to lock in Start")
	index := 1
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
	rf.print("CLNT", "Actioning command %v", command)

	// Append to log
	newEntry := LogEntry{LogIndex: index, Term: term, Value: command}
	rf.print("LOGS", "Appending %v to log (starting at spot %v)", newEntry, len(rf.log))
	rf.log = append(rf.log, newEntry)
	rf.persist(nil)
	rf.matchIndex[rf.me] = rf.log[len(rf.log)-1].LogIndex
	// Tell all other servers to append
	for i := range rf.peers {
		if i != rf.me {
			go rf.sendAppendEntries(i)
		}
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
		go rf.sendAppendEntries(server)
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
		for i := range rf.nextIndex {
			rf.nextIndex[i] = 1
			rf.matchIndex[i] = 0
		}
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

func (rf *Raft) doApply() {
	toCommit := <- rf.commitCh
	// Check if this hasn't already been applied
	if toCommit.lastIndex > rf.lastAppliedCompleted {
		for i := len(toCommit.entries) - (toCommit.lastIndex - rf.lastAppliedCompleted); i < len(toCommit.entries); i++ {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command: toCommit.entries[i].Value,
				CommandIndex: toCommit.entries[i].LogIndex,
				SnapshotValid: false
			}
		}
	}

	rf.doApply()
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
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0
	data := rf.persister.ReadRaftState()
	rf.readPersist(data)
	rf.lastSnapshot = rf.persister.ReadSnapshot()
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.lastAppliedCompleted = 0
	rf.role = FOLLOWER
	rf.applyCh = applyCh
	rf.commitCh = make(chan ToCommit)

	// Leader state
	// TODO: This will have to be initialized to non-0 later
	rf.nextIndex = make([]int, len(peers))
	for i := range peers {
		rf.nextIndex[i] = 1
	}
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
	// TODO: maybe apply snapshot?

	rf.print("STRT", "started")
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.doApply()

	return rf
}
