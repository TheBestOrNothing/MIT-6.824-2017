package raftkv

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"labrpc"
	"raft"
	"sort"
	"sync"
	"time"
)

const Debug = 0

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
	Op     string //the operation
	Key    string
	Value  string
	Client uint32
	SeqNum uint32
}

type Entry struct {
	Op     string
	Value  string
	Index  int
	Client uint32
	SeqNum uint32
}

type RaftKV struct {
	sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data      map[string]map[uint64]Entry
	zipData   map[string]string
	committed map[uint32]uint32
	checkCode map[uint32]uint32
}

func (kv *RaftKV) CheckLeader(args *GetLeaderArgs, reply *GetLeaderReply) {
	term, isLeader := kv.rf.GetState()
	reply.WrongLeader = !isLeader
	reply.Term = term
	reply.Me = kv.me

	//reply.Committed = kv.committed[args.Client]
	//reply.CheckCode = kv.checkCode[args.Client]
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	//fmt.Printf("Get :%d - WrongLeader:%v,Err:%v  \n",
	//	kv.me, putReply.WrongLeader, putReply.Err)
	//defer func() {
	//	reply.Committed = kv.committed[args.Client]
	//	reply.CheckCode = kv.checkCode[args.Client]
	//}()

	reply.WrongLeader = false
	reply.Err = ErrNoKey
	reply.Value = ""

	kv.RLock()
	values := ""
	//To Check entry with SeqNum is applied
	value, ok1 := kv.data[args.Key]
	if !ok1 {
		kv.RUnlock()
		DPrintf("Server GET No Key:%v exist\n", args.Key)
		return
	}

	var keys U64Array
	for key := range value {
		keys = append(keys, key)
	}
	sort.Sort(keys)
	for _, k := range keys {
		if value[k].Op == "Get" {
			continue
		}
		values += value[k].Value
	}
	kv.RUnlock()
	reply.Err = OK
	reply.Value = values
	DPrintf("Server GET OP{%v, %v, %v} success\n", "Get", args.Key, values)

}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	defer func() {
		kv.RLock()
		reply.Committed = kv.committed[args.Client]
		reply.CheckCode = kv.checkCode[args.Client]
		kv.RUnlock()
	}()

	cmd := Op{
		Op:     args.Op,
		Key:    args.Key,
		Value:  args.Value,
		Client: args.Client,
		SeqNum: args.SeqNum}

	index, _, isLeader := kv.rf.Start(cmd)

	reply.WrongLeader = !isLeader
	reply.Err = ErrNoKey
	reply.Me = kv.me
	reply.Index = index

	if index == -1 {
		return
	}
	//fmt.Printf("Server:%d Put Start: - Op{Op:%v, Key:%v, Value:%v} index:%d Err:%v\n",
	//	kv.me, cmd.Op, cmd.Key, cmd.Value, index, reply.Err)
	t0 := time.Now()
	for time.Since(t0).Seconds() < 10 {
		time.Sleep(10 * time.Millisecond)
		_, isLeader = kv.rf.GetState()
		if !isLeader {
			reply.WrongLeader = !isLeader
			return
		}

		kv.RLock()
		entry, ok1 := kv.data[cmd.Key]
		if !ok1 {
			kv.RUnlock()
			continue
		}

		CliSeq := uint64(args.Client)<<32 + uint64(args.SeqNum)
		//fmt.Printf("CliSeq Server: Client:%v, SeqNum:%v, CltSeq:%v\n",
		//	args.Client, args.SeqNum, CliSeq)
		v, ok2 := entry[CliSeq]
		if !ok2 {
			kv.RUnlock()
			continue
		}
		//kv.printData()
		kv.RUnlock()
		reply.Err = OK
		DPrintf("Server PUT OP{%v, %v, %v} success {WrongL:%v, Err:%v, Me:%v,Index:%v}\n",
			args.Op, args.Key, args.Value, reply.WrongLeader, reply.Err, reply.Me, index)
		if v.Value != cmd.Value {
			fmt.Printf("Warning the value is not matched after put\n")
		}
		return
	}
	fmt.Printf("Warning: Server take more than 10 Sec to PutApp\n")
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
	//fmt.Printf("StartKVServer :%d\n", me)
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.data = make(map[string]map[uint64]Entry)
	kv.zipData = make(map[string]string)
	kv.committed = make(map[uint32]uint32)
	kv.checkCode = make(map[uint32]uint32)

	go func() {
		for msg := range kv.applyCh {
			kv.Lock()

			if msg.UseSnapshot {
				if msg.Snapshot == nil || len(msg.Snapshot) < 1 {
					kv.Unlock()
					continue
				}
				clt := uint32(0)
				seq := uint32(0)
				r := bytes.NewBuffer(msg.Snapshot)
				d := gob.NewDecoder(r)
				d.Decode(&kv.zipData)
				d.Decode(&clt)
				d.Decode(&seq)
				//kv.printZipData()

				for key := range kv.data {
					delete(kv.data, key)
				}

				for key := range kv.zipData {
					entry := make(map[uint64]Entry)
					CltSeq := uint64(clt)<<32 + uint64(seq)
					entry[CltSeq] = Entry{
						Value:  kv.zipData[key],
						Op:     "Put",
						Client: clt,
						Index:  msg.Index,
						SeqNum: seq,
					}
					kv.data[key] = entry

				}
				kv.committed[clt] = seq
				//kv.printData1(kv.me)
				kv.Unlock()
				continue
			}

			if cmd, ok := (msg.Command).(Op); ok {
				//fmt.Printf("KVRaft:%d - Op{Op:%v, Key:%v, Value:%v} index:%d\n",
				//	kv.me, cmd.Op, cmd.Key, cmd.Value, msg.Index)
				CliSeq := uint64(cmd.Client)<<32 + uint64(cmd.SeqNum)
				switch cmd.Op {
				case "Put":
					delete(kv.data, cmd.Key)
					entry := make(map[uint64]Entry)
					entry[CliSeq] = Entry{
						Value:  cmd.Value,
						Op:     cmd.Op,
						Client: cmd.Client,
						Index:  msg.Index,
						SeqNum: cmd.SeqNum}

					kv.data[cmd.Key] = entry
					kv.committed[cmd.Client] = cmd.SeqNum
				case "Append":
					entry, ok := kv.data[cmd.Key]
					if !ok {
						entry = make(map[uint64]Entry)
					}
					entry[CliSeq] = Entry{
						Value:  cmd.Value,
						Op:     cmd.Op,
						Client: cmd.Client,
						Index:  msg.Index,
						SeqNum: cmd.SeqNum}
					kv.committed[cmd.Client] = cmd.SeqNum
				case "Get":
					entry, ok := kv.data[cmd.Key]
					if !ok {
						entry = make(map[uint64]Entry)
					}
					entry[CliSeq] = Entry{
						Value:  cmd.Value,
						Op:     cmd.Op,
						Client: cmd.Client,
						Index:  msg.Index,
						SeqNum: cmd.SeqNum}
					kv.data[cmd.Key] = entry
					kv.checkCode[cmd.Client] = cmd.SeqNum
				}

				if maxraftstate != -1 && persister.RaftStateSize() > maxraftstate {
					//zip kv.data and make a snapshort of data
					kv.zip(msg.Index, cmd.Client, cmd.SeqNum)
					//kv.printZipData()
					w := new(bytes.Buffer)
					e := gob.NewEncoder(w)
					e.Encode(kv.zipData)
					e.Encode(cmd.Client)
					e.Encode(cmd.SeqNum)
					data := w.Bytes()
					kv.rf.MakeSnapShot(msg.Index, data)
				}
			} else {
				fmt.Printf("The OP can not dry from msg\n")
			}
			//kv.printData(kv.me)

			kv.Unlock()
		} //end for
	}()

	return kv
}

func (kv *RaftKV) printData1(me int) {
	DPrintf("%d Printing data ...\n", me)
	var keys []string
	for key := range kv.data {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		for _, v := range kv.data[key] {
			fmt.Printf("{%v, %v}", key, v.Value)
		}
	}

	fmt.Printf("\n")
}
func (kv *RaftKV) printData(me int) {
	//if _, leader := kv.rf.GetState(); !leader {
	//	return
	//}
	DPrintf("%d Printing data ...\n", me)
	for key, values := range kv.data {
		fmt.Printf("Key:%v \n", key)

		var keys U64Array
		for key := range values {
			keys = append(keys, key)
		}
		sort.Sort(keys)
		for _, k := range keys {
			//fmt.Printf("{%v - %v, %v, %v, %v} ",
			//	k, entry[k].Op, entry[k].Value, entry[k].Index, entry[k].Client)
			//fmt.Printf("{%v - %v, %v, %v} ",
			//	k, values[k].Op, values[k].Value, values[k].Index)
			fmt.Printf("{%v, %v, %v} ",
				values[k].Op, values[k].Value, values[k].Index)
		}
		fmt.Printf("\n")
	}
}

func (kv *RaftKV) zip(idx int, clt uint32, seq uint32) {

	for key := range kv.data {
		//Step 1 : Get all the key and values from kv.data
		values := ""
		value, _ := kv.data[key]

		var CltSeqs U64Array
		for seq := range value {
			CltSeqs = append(CltSeqs, seq)
		}
		sort.Sort(CltSeqs)
		for _, s := range CltSeqs {
			if value[s].Op == "Get" {
				continue
			}
			values += value[s].Value
		}
		kv.zipData[key] = values
	}
}

func (kv *RaftKV) printZipData() {
	DPrintf("Printing zipData ...\n")
	var keys []string
	for key := range kv.zipData {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Printf("{%v, %v}", k, kv.zipData[k])
	}
	fmt.Printf("\n")
}
