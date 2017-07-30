package raftkv

import "labrpc"
import "math/rand"
import "time"

//import "fmt"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	term      int
	leader    int
	latestNum uint32
	committed uint32
	cltId     uint32
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.leader = 0
	ck.term = 0
	ck.latestNum = 0
	ck.committed = 0
	ck.cltId = rand.Uint32()
	DPrintf("MakeClerk %v\n", ck.cltId)
	return ck
}

func (ck *Clerk) CheckOneLeader() {
	//DPrintf("Are you in  CheckOneLeader\n")
	args := &GetLeaderArgs{}
	replys := make([]GetLeaderReply, len(ck.servers))
	//leaders := make([]int, len(ck.servers))
	leaders := make(map[int][]int)
	for si := 0; si < len(ck.servers); si++ {
		ok := ck.servers[si].Call("RaftKV.CheckLeader", args, &replys[si])
		if !ok {
			replys[si].Err = ErrNoKey
			continue
		}

		if !replys[si].WrongLeader {
			t := replys[si].Term
			leaders[t] = append(leaders[t], si)
		}
	}
	lastTermWithLeader := -1
	for t, ls := range leaders {
		if len(ls) > 1 {
			DPrintf("Waring: There are two leader in term %d\n", t)
			ck.leader = -1
			return
		}

		if t > lastTermWithLeader {
			lastTermWithLeader = t
			ck.term = t
		}
	}

	if len(leaders) != 0 {
		ck.leader = leaders[lastTermWithLeader][0]
		return
	}

	ck.leader = -1
	return
}

func (ck *Clerk) TheOne() {
	ck.CheckOneLeader()
	for ck.leader == -1 {
		time.Sleep(10 * time.Millisecond)
		ck.CheckOneLeader()
	}
}

//One simple and fairly efficient one is to give each client a unique identifier,
//and then have them tag each request with a monotonically increasing sequence number.
//If a client re-sends a request, it re-uses the same sequence number.
//Your server keeps track of the latest sequence number it has seen for each client,
//and simply ignores any operation that it has already seen.
func (ck *Clerk) isCommitted(SeqNum uint32, CheckCode uint32, Key string) bool {
	//DPrintf("Are you in  isCommitted\n")
	ck.TheOne()
	args := &PutAppendArgs{
		Key:    Key,
		Value:  "",
		Op:     "Get",
		Client: ck.cltId,
		SeqNum: CheckCode,
	}

	reply := &PutAppendReply{}
	//DPrintf("Client Put From Server1 {%v, %v, %v} -- {WrongL:%v, Err:%v, Me:%v}\n",
	//	args.Op, args.Key, args.Value, reply.WrongLeader, reply.Err, reply.Me)
	ok := ck.servers[ck.leader].Call("RaftKV.PutAppend", args, reply)

	if !ok {
		time.Sleep(10 * time.Millisecond)
	}

	if reply.Committed == SeqNum {
		return true
	}

	if reply.CheckCode == CheckCode {
		return false
	}

	return ck.isCommitted(SeqNum, CheckCode, Key)
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	//DPrintf("Client Get input0 latestNum:%d \n", ck.latestNum)

	ck.isCommitted(rand.Uint32(), rand.Uint32(), key)
	args := &GetArgs{
		Key:    key,
		Client: ck.cltId,
	}

	//DPrintf("Client Get input1 args:%v \n", args)
ReGet:
	reply := &GetReply{}
	//DPrintf("Client Get input args:%v, reply:%v\n", args, reply)
	ok := ck.servers[ck.leader].Call("RaftKV.Get", args, reply)
	//DPrintf("Client Get form server %v\n", reply)
	if !ok {
		goto ReGet
	}

	if ok && reply.Err == OK {
		DPrintf("Client GET OP{%v, %v, %v} success {Committed:%d}\n",
			"Get", args.Key, reply.Value, args.Committed)
		return reply.Value
	}

	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.latestNum++
	done := ck.latestNum

	args := &PutAppendArgs{
		Key:    key,
		Value:  value,
		Op:     op,
		Client: ck.cltId,
		SeqNum: ck.latestNum,
	}

RePutAppend:
	reply := &PutAppendReply{}
	//DPrintf("Client Put From Server1 {%v, %v, %v} -- {WrongL:%v, Err:%v, Me:%v}\n",
	//	args.Op, args.Key, args.Value, reply.WrongLeader, reply.Err, reply.Me)
	ok := ck.servers[ck.leader].Call("RaftKV.PutAppend", args, reply)
	//DPrintf("Client Put From Server2 {%v, %v, %v} -- {WrongL:%v, Err:%v, Me:%v}\n",
	//	args.Op, args.Key, args.Value, reply.WrongLeader, reply.Err, reply.Me)
	if ok && reply.Err == OK {
		DPrintf("Client PUT OP{%v, %v, %v} success\n", op, key, value)
		ck.committed = done
		return
	}

	DPrintf("Client REPUTAPPEND\n")
	if !ok || reply.Index != -1 {
		if ck.isCommitted(ck.latestNum, rand.Uint32(), key) {
			ck.committed = done
			return
		} else {
			goto RePutAppend
		}
	}

	if reply.Index == -1 {
		ck.TheOne()
		goto RePutAppend
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
