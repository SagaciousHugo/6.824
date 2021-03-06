package raftkv

import (
	"labrpc"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers  []*labrpc.ClientEnd
	clientId int64
	lastOpId int64
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
	ck.clientId = nrand()
	ck.lastOpId = 0
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	ck.lastOpId++
	args := GetArgs{
		Key:      key,
		ClientId: ck.clientId,
		OpId:     ck.lastOpId,
	}
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply GetReply
			ok := srv.Call("KVServer.Get", &args, &reply)
			if ok && (reply.Result == OK || reply.Result == ErrNoKey) {
				return reply.Value
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.lastOpId++
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		OpId:     ck.lastOpId,
		ClientId: ck.clientId,
	}
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply PutAppendReply
			ok := srv.Call("KVServer.PutAppend", &args, &reply)
			if ok && reply.Result == OK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
