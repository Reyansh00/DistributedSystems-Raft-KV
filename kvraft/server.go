package kvraft

import (
	"../labgob"
	"../labrpc"
	"log"
	"../raft"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type string
	Key string
	Value string
	ClientId int64
	RequestId int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	store map[string]string
	lastReq map[int64]int // client id -> last seq processed
	waitCh map[int]chan Op
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		Type: "Get",
		Key: args.Key,
		ClientId : args.ClientId,
		RequestId: args.RequestId,
	}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader{
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	ch := make(chan Op, 1)
	kv.waitCh[index] = ch
	kv.mu.Unlock()
	select {
	case committedOp := <-ch:
	    if committedOp.ClientId != op.ClientId || committedOp.RequestId != op.RequestId {
			reply.Err = ErrWrongLeader
			kv.mu.Lock()
			delete(kv.waitCh, index)
			kv.mu.Unlock()
			return
	    }
	case <-time.After(500 * time.Millisecond):
	    reply.Err = ErrWrongLeader
		// kv.mu.Lock()
		// delete(kv.waitCh, index)
		// kv.mu.Unlock()
	    return
	}
	kv.mu.Lock()
	delete(kv.waitCh, index)
	val, exists := kv.store[op.Key]
	if exists {
    	reply.Err = OK
    	reply.Value = val
	} else {
	    reply.Err = ErrNoKey
	}
	kv.mu.Unlock()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		Type: args.Op,
		Key: args.Key,
		Value: args.Value,
		ClientId : args.ClientId,
		RequestId: args.RequestId,
	}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader{
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	ch := make(chan Op, 1)
	kv.waitCh[index] = ch
	kv.mu.Unlock()
	select {
	case committedOp := <-ch:
		if committedOp.ClientId != op.ClientId || committedOp.RequestId != op.RequestId {
			reply.Err = ErrWrongLeader
			kv.mu.Lock()
			delete(kv.waitCh, index)
			kv.mu.Unlock()
			return
		}
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrWrongLeader
		// kv.mu.Lock()
		// delete(kv.waitCh, index)
		// kv.mu.Unlock()
		return
	}
	kv.mu.Lock()
	delete(kv.waitCh, index)
	kv.mu.Unlock()
	reply.Err = OK
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) applyLoop() {
    for {
        msg := <- kv.applyCh

        if msg.CommandValid {
			op := msg.Command.(Op)
			kv.mu.Lock()
			if op.Type == "Get" {
				// nothing
			}else{
				last, seen := kv.lastReq[op.ClientId]
				if !seen || op.RequestId > last{
					switch op.Type {
					case "Put":
					    kv.store[op.Key] = op.Value
					case "Append":
					    kv.store[op.Key] += op.Value
					}
					kv.lastReq[op.ClientId] = op.RequestId
				}
			}
			ch, ok := kv.waitCh[msg.CommandIndex]
			kv.mu.Unlock()
			if ok {
				ch <- op
			}
        }
    }
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.store = make(map[string]string)
	kv.lastReq = make(map[int64]int)
	kv.waitCh = make(map[int]chan Op)
	go kv.applyLoop()

	return kv
}
