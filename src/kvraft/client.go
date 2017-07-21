package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"
import "time"
import "fmt"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
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
	return ck
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

	// You will have to modify this function.
	args := &GetArgs{
		Key: key,
	}
	value := ""
	t0 := time.Now()
	for time.Since(t0).Seconds() < 10 {
		replys := make([]GetReply, len(ck.servers))
		for si := 0; si < len(ck.servers); si++ {
			ok := ck.servers[si].Call("RaftKV.Get", args, &replys[si])
			if ok && replys[si].WrongLeader {
				continue
			}
			if replys[si].Err == OK {
				return replys[si].Value
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return value
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
	// You will have to modify this function.
	args := &PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
	}

	t0 := time.Now()
	for time.Since(t0).Seconds() < 10 {
		replys := make([]PutAppendReply, len(ck.servers))
		for si := 0; si < len(ck.servers); si++ {
			ok := ck.servers[si].Call("RaftKV.PutAppend", args, &replys[si])
			//fmt.Printf("kvRaft:%d, ok:%v {%v, %v} \n",
			//	replys[si].Me, ok, replys[si].WrongLeader, replys[si].Err)
			if ok && replys[si].WrongLeader {
				continue
			}

			if replys[si].Err == OK {
				fmt.Printf("OP{%v, %v, %v} PutAppend success\n", op, key, value)
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
