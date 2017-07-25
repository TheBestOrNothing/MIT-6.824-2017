package raftkv

import (
	"encoding/gob"
	"fmt"
	"labrpc"
	"raft"
	"sort"
	"sync"
	"time"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		fmt.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op    string //the operation
	Key   string
	Value string
}

type Entry struct {
	Op    string
	Value string
}

type RaftKV struct {
	sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data map[string]map[int]Entry
}

func (kv *RaftKV) CheckLeader(args *GetLeaderArgs, reply *GetLeaderReply) {
	term, isLeader := kv.rf.GetState()
	reply.WrongLeader = !isLeader
	reply.Term = term
	reply.Me = kv.me
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	reply.Value = ""

	putArgs := &PutAppendArgs{
		Op:    "Get",
		Key:   args.Key,
		Value: "",
	}
	putReply := &PutAppendReply{}
	kv.PutAppend(putArgs, putReply)
	//fmt.Printf("Get :%d - WrongLeader:%v,Err:%v  \n",
	//	kv.me, putReply.WrongLeader, putReply.Err)
	reply.WrongLeader = putReply.WrongLeader
	reply.Err = putReply.Err
	if reply.Err != OK {
		return
	}

	kv.RLock()
	values := ""
	entry, _ := kv.data[args.Key]
	var keys []int
	for key := range entry {
		keys = append(keys, key)
	}
	sort.Ints(keys)
	for _, k := range keys {
		if entry[k].Op == "Get" {
			continue
		}
		values += entry[k].Value
	}
	kv.RUnlock()
	reply.Value = values
	//fmt.Printf("Get From leader:%d - Op{Key:%v, Value:%v} \n",
	//	kv.me, args.Key, values)
	DPrintf("Server GET OP{%v, %v, %v} success\n", "Get", args.Key, values)

}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {

	cmd := Op{Op: args.Op, Key: args.Key, Value: args.Value}
	index, _, isLeader := kv.rf.Start(cmd)

	reply.WrongLeader = !isLeader
	reply.Err = ErrNoKey
	reply.Me = kv.me

	if !isLeader {
		return
	}

	//fmt.Printf("Server:%d Put Start: - Op{Op:%v, Key:%v, Value:%v} index:%d Err:%v\n",
	//	kv.me, cmd.Op, cmd.Key, cmd.Value, index, reply.Err)

	t0 := time.Now()
	for time.Since(t0).Seconds() < 5 {
		time.Sleep(10 * time.Millisecond)
		kv.RLock()
		entry, ok1 := kv.data[cmd.Key]
		if !ok1 {
			kv.RUnlock()
			continue
		}
		v, ok2 := entry[index]
		if !ok2 {
			kv.RUnlock()
			continue
		}
		//kv.printData()
		kv.RUnlock()
		reply.Err = OK
		//fmt.Printf("Leader:%d - Op{Op:%v, Key:%v, Value:%v} index:%d Err:%v\n",
		//	kv.me, cmd.Op, cmd.Key, cmd.Value, index, reply.Err)
		DPrintf("Server PUT OP{%v, %v, %v} success {WrongL:%v, Err:%v, Me:%v,Index:%v}\n",
			args.Op, args.Key, args.Value, reply.WrongLeader, reply.Err, reply.Me, index)
		if v.Value != cmd.Value {
			fmt.Printf("Warning the value is not matched after put\n")
		}
		return
	}
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.data = make(map[string]map[int]Entry)

	go func() {
		for msg := range kv.applyCh {
			kv.Lock()
			if cmd, ok := (msg.Command).(Op); ok {
				//kv.log[msg.Index] = cmd
				//fmt.Printf("KVRaft:%d - Op{Op:%v, Key:%v, Value:%v} index:%d\n",
				//	kv.me, cmd.Op, cmd.Key, cmd.Value, msg.Index)
				switch cmd.Op {
				case "Put":
					delete(kv.data, cmd.Key)
					entry := make(map[int]Entry)
					entry[msg.Index] = Entry{Value: cmd.Value, Op: cmd.Op}
					kv.data[cmd.Key] = entry
				default:
					//case "Append": case "Get":
					entry, ok := kv.data[cmd.Key]
					if !ok {
						entry = make(map[int]Entry)
					}
					entry[msg.Index] = Entry{Value: cmd.Value, Op: cmd.Op}
				}
			} else {
				fmt.Printf("The OP can not dry from msg\n")
			}
			kv.Unlock()
			kv.printData(kv.me)
		}
	}()

	return kv
}

func (kv *RaftKV) printData(me int) {
	if _, leader := kv.rf.GetState(); !leader {
		return
	}
	fmt.Printf("%d Printing data ...\n", me)
	kv.RLock()
	for key, entry := range kv.data {
		fmt.Printf("Key:%v \n", key)

		var keys []int
		for key := range entry {
			keys = append(keys, key)
		}
		sort.Ints(keys)
		for _, k := range keys {
			fmt.Printf("{%v - %v, %v} ", k, entry[k].Op, entry[k].Value)
		}
		fmt.Printf("\n")
	}
	kv.RUnlock()
}
