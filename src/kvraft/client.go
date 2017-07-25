package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"
import "time"

//import "fmt"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	term   int
	leader int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.leader = 0
	ck.term = 0
	return ck
}

func (ck *Clerk) CheckOneLeader() {
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
	args := &GetArgs{
		Key: key,
	}

ReGet:
	reply := &GetReply{}
	ok := ck.servers[ck.leader].Call("RaftKV.Get", args, reply)

	if !ok {
		time.Sleep(10 * time.Millisecond)
		goto ReGet
	}
	if reply.WrongLeader {
		ck.TheOne()
		goto ReGet
	}
	if reply.Err != OK {
		goto ReGet
	}
	DPrintf("Client GET OP{%v, %v, %v} success\n", "Get", key, reply.Value)
	return reply.Value
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
	args := &PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
	}

RePutAppend:
	reply := &PutAppendReply{}
	//DPrintf("Client Put From Server1 {%v, %v, %v} -- {WrongL:%v, Err:%v, Me:%v}\n",
	//	args.Op, args.Key, args.Value, reply.WrongLeader, reply.Err, reply.Me)
	ok := ck.servers[ck.leader].Call("RaftKV.PutAppend", args, reply)
	//DPrintf("Client Put From Server2 {%v, %v, %v} -- {WrongL:%v, Err:%v, Me:%v}\n",
	//	args.Op, args.Key, args.Value, reply.WrongLeader, reply.Err, reply.Me)
	if !ok {
		DPrintf("Client PUT Server Losting ? ? ?\n")
		time.Sleep(10 * time.Millisecond)
		goto RePutAppend
	}
	if reply.WrongLeader {
		DPrintf("Client PUT Finding A New Leader\n")
		ck.TheOne()
		goto RePutAppend
	}
	if reply.Err != OK {
		goto RePutAppend
	}
	DPrintf("Client PUT OP{%v, %v, %v} success\n", op, key, value)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
