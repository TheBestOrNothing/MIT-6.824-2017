package raftkv

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Client uint32
	SeqNum uint32
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
	Me          int
	Index       int
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Client    uint32
	Committed uint32
	CheckCode uint32
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Client    uint32
	Committed uint32
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
	Committed   uint32
	CheckCode   uint32
}

type GetLeaderArgs struct {
	Me     int
	Client uint32
}

type GetLeaderReply struct {
	WrongLeader bool
	Err         Err
	Me          int
	Term        int
	Committed   uint32
	CheckCode   uint32
}

type U64Array []uint64

func (a U64Array) Len() int           { return len(a) }
func (a U64Array) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a U64Array) Less(i, j int) bool { return a[i] < a[j] }
