package shardmaster

import (
	"container/heap"
	"fmt"
	"log"
	"sync"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Wait struct {
	l sync.RWMutex
	m map[string]chan OpResult
}

// New creates a Wait.
func NewWait() *Wait {
	return &Wait{m: make(map[string]chan OpResult)}
}

func (w *Wait) Register(info OpInfo) <-chan OpResult {
	w.l.Lock()
	defer w.l.Unlock()
	key := fmt.Sprintf("%d_%d", info.getClientId(), info.getOpId())
	ch := w.m[key]
	if ch != nil {
		close(ch)
	}
	ch = make(chan OpResult, 1)
	w.m[key] = ch
	return ch
}

func (w *Wait) Unregister(info OpInfo) {
	w.l.Lock()
	defer w.l.Unlock()
	key := fmt.Sprintf("%d_%d", info.getClientId(), info.getOpId())
	ch := w.m[key]
	delete(w.m, key)
	if ch != nil {
		close(ch)
	}
}

func (w *Wait) Trigger(result OpResult) {
	w.l.RLock()
	defer w.l.RUnlock()
	key := fmt.Sprintf("%d_%d", result.ClientId, result.OpId)
	ch := w.m[key]
	if ch != nil {
		ch <- result
	}
}

/* average divide shards for groups (result from high to low )
shardNum fixed to 10
groupNum shardResult:
1 	[10]
2 	[5 5]
3 	[4 3 3]
4 	[3 3 2 2]
5 	[2 2 2 2 2]
6 	[2 2 2 2 1 1]
7 	[2 2 2 1 1 1 1]
8 	[2 2 1 1 1 1 1 1]
9 	[2 1 1 1 1 1 1 1 1]
10 	[1 1 1 1 1 1 1 1 1 1]
11  [1 1 1 1 1 1 1 1 1 1 0]
12  [1 1 1 1 1 1 1 1 1 1 0 0]
*/
func shardDivide(shardNum, groupNum int) []int {
	if groupNum == 0 {
		return []int{0}
	}
	res := make([]int, groupNum)
	for i := 0; i < shardNum; i++ {
		res[i%groupNum]++
	}
	return res
}

type GroupShard struct {
	Index  int
	Gid    int
	Shards []int
}

func NewHeapGroupShard(groups map[int][]string) *heapGroupShard {
	h := &heapGroupShard{m: make(map[int]*GroupShard)}
	for gid := range groups {
		h.addGs(gid, -1)
	}
	return h
}

type heapGroupShard struct {
	data []*GroupShard
	m    map[int]*GroupShard
}

func (h heapGroupShard) Len() int { return len(h.data) }
func (h heapGroupShard) Less(i, j int) bool {
	if len(h.data[i].Shards) != len(h.data[j].Shards) {
		return len(h.data[i].Shards) > len(h.data[j].Shards)
	} else {
		return h.data[i].Gid > h.data[j].Gid
	}
}
func (h heapGroupShard) Swap(i, j int) {
	h.data[i], h.data[j] = h.data[j], h.data[i]
	h.data[i].Index = i
	h.data[j].Index = j
}

func (h *heapGroupShard) Push(x interface{}) {
	gs := x.(*GroupShard)
	gs.Index = len(h.data)
	h.data = append(h.data, gs)
}

func (h *heapGroupShard) Pop() interface{} {
	old := h
	n := len(old.data)
	x := old.data[n-1]
	x.Index = -1
	h.data = old.data[0 : n-1]
	delete(h.m, x.Gid)
	return x
}

func (h *heapGroupShard) addGs(gid, shard int) {
	gs, ok := h.m[gid]
	if ok {
		gs.Shards = append(gs.Shards, shard)
		heap.Fix(h, gs.Index)
	} else {
		if shard == -1 {
			gs = &GroupShard{-1, gid, []int{}}
		} else {
			gs = &GroupShard{-1, gid, []int{shard}}
		}
		h.m[gid] = gs
		heap.Push(h, gs)
	}
}
