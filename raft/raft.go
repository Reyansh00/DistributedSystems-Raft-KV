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

import "sync"
import "sync/atomic"
import "../labrpc"
import "time"
import "math/rand"
import "bytes"
import "../labgob"



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
const (
	Follower = iota
	Candidate
	Leader
)

type LogEntry struct {
	Term int
	Command interface{}
}
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor int
	state int //0->follower 1->candidate 2->leader
	electionTimer time.Time
	log []LogEntry
	// isLeader bool
	commitIndex int
	lastApplied int
	applyCh chan ApplyMsg
	nextIndex []int
	matchIndex []int
	applyCond *sync.Cond
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.state==Leader
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
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
	if len(data) < 1 { //
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil ||
	   d.Decode(&votedFor) != nil ||
	   d.Decode(&log) != nil {
	  return
	   }
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = log
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	Id int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

type AppendEntriesArgs struct {
    Term int
    LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
    Term int
	WriteSuccess bool
	XTerm int
	XIndex int
	XLen int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
    rf.mu.Lock()
    defer rf.mu.Unlock()

    if args.Term < rf.currentTerm {
        reply.Term = rf.currentTerm
        reply.WriteSuccess = false
        return
    }

    rf.currentTerm = args.Term
    rf.state = Follower
	rf.persist()
    rf.electionTimer = time.Now()

    // Check PrevLogIndex/PrevLogTerm for ALL RPCs, including heartbeats
    if args.PrevLogIndex >= len(rf.log) {
        reply.Term = rf.currentTerm
        reply.WriteSuccess = false
		reply.XTerm = -1
		reply.XLen = len(rf.log)
		reply.WriteSuccess = false
        return
    }
    if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
        reply.Term = rf.currentTerm
        reply.WriteSuccess = false
		reply.XTerm = rf.log[args.PrevLogIndex].Term
		xi := args.PrevLogIndex
    	for xi > 0 && rf.log[xi-1].Term == reply.XTerm {
    	    xi--
    	}
		reply.XIndex = xi
        return
    }

    // Append/overwrite entries (harmless if Entries is empty)
    for i, entry := range args.Entries {
        idx := args.PrevLogIndex + 1 + i
        if idx < len(rf.log) {
            if rf.log[idx].Term != entry.Term {
                rf.log = rf.log[:idx]
                rf.log = append(rf.log, args.Entries[i:]...)
				rf.persist()
                break
            }
        } else {
            rf.log = append(rf.log, args.Entries[i:]...)
            rf.persist()
            break
        }
    }

    // Update commitIndex
    if args.LeaderCommit > rf.commitIndex {
        lastIndex := len(rf.log) - 1
        if args.LeaderCommit < lastIndex {
            rf.commitIndex = args.LeaderCommit
        } else {
            rf.commitIndex = lastIndex
        }
        rf.applyCond.Signal()
    }

    reply.Term = rf.currentTerm
    reply.WriteSuccess = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool{
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	if args.Term < rf.currentTerm{
		reply.Term = rf.currentTerm
		rf.persist()
		reply.VoteGranted = false
		return
	}
	myLastIndex := len(rf.log) - 1
	myLastTerm := rf.log[myLastIndex].Term

	upToDate := false

	if args.LastLogTerm > myLastTerm {
		upToDate = true
	} else if args.LastLogTerm == myLastTerm &&
		args.LastLogIndex >= myLastIndex {
		upToDate = true
	}

	if args.Term > rf.currentTerm{
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
		rf.persist()
		reply.Term = rf.currentTerm
	}
	if !upToDate {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if rf.votedFor == -1 || rf.votedFor == args.Id {
		rf.votedFor = args.Id
		rf.electionTimer = time.Now()
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		rf.persist()
		return
	}
	reply.VoteGranted = false
	reply.Term = rf.currentTerm
	rf.persist()
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
func(rf *Raft) broadcastAppendEntries() { // to brodcast append entries
	// Implementation for broadcasting append entries
	rf.mu.Lock()
    if rf.state != Leader {   
        rf.mu.Unlock()
        return
    }
    rf.mu.Unlock()
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			rf.mu.Lock()
			next := rf.nextIndex[server]
			if next > len(rf.log) {
				next = len(rf.log)
			}
			prevIdx := next - 1
			prevTerm := rf.log[prevIdx].Term
			entries := append([]LogEntry{}, rf.log[next:]...)
			term := rf.currentTerm
			leaderCommit := rf.commitIndex
			rf.mu.Unlock()

			args := AppendEntriesArgs{
			    Term:         term,
			    LeaderId:     rf.me,
			    PrevLogIndex:  prevIdx,
			    PrevLogTerm:   prevTerm,
			    Entries:       entries,
			    LeaderCommit:  leaderCommit,
			}
			reply := AppendEntriesReply{}
			ok := rf.sendAppendEntries(server, &args, &reply)
			if !ok {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.state != Leader || term != rf.currentTerm {
			    return
			}
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.state = Follower
				rf.votedFor = -1
				rf.electionTimer = time.Now()
				rf.persist()
				return
			}
			if reply.WriteSuccess {
				if len(entries) > 0 {
					rf.matchIndex[server] = prevIdx + len(entries)
					rf.nextIndex[server] = rf.matchIndex[server] + 1
				}
				for N := len(rf.log)-1; N > rf.commitIndex; N-- {
					count := 1 // leader
					for i := range rf.peers {
						if i != rf.me && rf.matchIndex[i] >= N {
							count++
						}
					}
					if count > len(rf.peers)/2 &&
						rf.log[N].Term == rf.currentTerm {
						rf.commitIndex = N
						rf.applyCond.Signal()
						break
					}
				}
			}else {
				if reply.XTerm == -1 {
					// follower log too short
					rf.nextIndex[server] = reply.XLen
				} else {
					// find if leader has XTerm
					ni := prevIdx
					for ni > 0 && rf.log[ni].Term != reply.XTerm {
						ni--
					}
					if rf.log[ni].Term == reply.XTerm {
						rf.nextIndex[server] = ni + 1
					} else {
						rf.nextIndex[server] = reply.XIndex
					}
				}
			}
		}(i)
	}
}

func (rf *Raft) applier() {
    for !rf.killed() {
        rf.mu.Lock()
        for rf.lastApplied >= rf.commitIndex {
            rf.applyCond.Wait()
        }
        // everything up to commitIndex
        var msgs []ApplyMsg
        for rf.lastApplied < rf.commitIndex {
            rf.lastApplied++
            msgs = append(msgs, ApplyMsg{
                CommandValid: true,
                Command:      rf.log[rf.lastApplied].Command,
                CommandIndex: rf.lastApplied,
            })
        }
        rf.mu.Unlock()
        for _, msg := range msgs {
            rf.applyCh <- msg
        }
    }
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	// isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return -1, -1, false
	}

	term = rf.currentTerm

	entry := LogEntry{
		Term:    term,
		Command: command,
	}

	rf.log = append(rf.log, entry)
	index = len(rf.log) -1
	rf.matchIndex[rf.me] = index
	rf.nextIndex[rf.me] = index + 1
	rf.persist()
	
	go rf.broadcastAppendEntries()
	return index, term, true
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker(){
	for !rf.killed(){
		timeout := time.Duration(350+rand.Intn(300))*time.Millisecond //check
		rf.mu.Lock()
		if rf.state != Leader && time.Since(rf.electionTimer)>=timeout{
			rf.mu.Unlock()
			rf.startElection()
		}else{
			rf.mu.Unlock()
		}
		rf.mu.Lock()
		leader := rf.state == Leader
		rf.mu.Unlock()
		if leader {
			go rf.broadcastAppendEntries()
		}
		time.Sleep(50*time.Millisecond)
	}
}

func (rf *Raft) startElection(){
	rf.mu.Lock()
	rf.state = Candidate
	rf.currentTerm++
	termStarted := rf.currentTerm
	rf.votedFor = rf.me
	votes := 1
	rf.electionTimer = time.Now()
	lastlogindex := len(rf.log)-1
	lastlogterm := rf.log[lastlogindex].Term
	rf.persist()
	rf.mu.Unlock()
	for i := range rf.peers {
		if i == rf.me{
			continue
		}
		go func(server int){
			args := RequestVoteArgs{
				Term: termStarted,
				Id : rf.me,
				LastLogIndex: lastlogindex,
				LastLogTerm: lastlogterm,
			}
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(server, &args, &reply)
			if !ok{
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.state = Follower
				rf.votedFor = -1
				rf.persist()
				return
			}
			if rf.currentTerm != termStarted {
			    return
			}
			if rf.state != Candidate {
				return
			}
			if reply.VoteGranted {
				votes++
				if rf.state==Candidate && votes > len(rf.peers)/2 {
					rf.state = Leader
					rf.electionTimer = time.Now()
					for i := range rf.peers {
						rf.nextIndex[i] = len(rf.log)
						rf.matchIndex[i] = 0
					}
					rf.matchIndex[rf.me] = len(rf.log)-1
					go rf.broadcastAppendEntries()
					return
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = Follower
	rf.electionTimer = time.Now()
	rf.log = []LogEntry{{Term: 0}}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.readPersist(persister.ReadRaftState())
	go rf.applier()
	go rf.ticker()
	// initialize from state persisted before a crash


	return rf
}
