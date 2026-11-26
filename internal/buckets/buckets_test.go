package buckets

import (
	"fmt"
	"strconv"
	"testing"
	"time"
)

func TestKeyUsesOnlyThePrefix(t *testing.T) {
	a := Key("shared preamble and document | question one", 30, 64)
	b := Key("shared preamble and document | question two", 30, 64)
	if a != b {
		t.Fatalf("same first 30 bytes, different buckets: %d %d", a, b)
	}
	for i := range 100 {
		if k := Key(fmt.Sprintf("doc %d", i), 2048, 7); k < 0 || k >= 7 {
			t.Fatalf("bucket %d out of range", k)
		}
	}
}

func TestAssignIsEvenEnough(t *testing.T) {
	m, _ := Assign(nil, []string{"a", "b"}, 64, 2048)
	c := m.Counts()
	if c["a"]+c["b"] != 64 || c["a"] < 20 || c["b"] < 20 {
		t.Fatalf("uneven split: %v", c)
	}
	if m.Version != 1 {
		t.Fatalf("version %d", m.Version)
	}
}

func TestAssignScaleUpMovesOnlyToTheNewReplica(t *testing.T) {
	m2, _ := Assign(nil, []string{"a", "b"}, 256, 2048)
	m3, changed := Assign(m2, []string{"a", "b", "c"}, 256, 2048)
	if !changed || m3.Version != m2.Version+1 {
		t.Fatalf("changed=%v version %d", changed, m3.Version)
	}
	moved := 0
	for b := range m3.Owners {
		if m3.Owners[b] != m2.Owners[b] {
			moved++
			if m3.Owners[b] != "c" {
				t.Fatalf("bucket %d moved to %s, not to the new replica", b, m3.Owners[b])
			}
		}
	}
	if moved < 256/3-30 || moved > 256/3+30 {
		t.Fatalf("moved %d of 256, expected about a third", moved)
	}
}

func TestAssignScaleDownKeepsOthers(t *testing.T) {
	m3, _ := Assign(nil, []string{"a", "b", "c"}, 128, 2048)
	m2, _ := Assign(m3, []string{"a", "b"}, 128, 2048)
	for b := range m2.Owners {
		if m3.Owners[b] != "c" && m2.Owners[b] != m3.Owners[b] {
			t.Fatalf("bucket %d of a remaining replica moved", b)
		}
		if m2.Owners[b] == "c" {
			t.Fatalf("bucket %d still on the removed replica", b)
		}
	}
}

func TestAssignKeepsRebalancedOwners(t *testing.T) {
	m, _ := Assign(nil, []string{"a", "b"}, 16, 2048)
	// move a bucket of a to b by hand, as Rebalance would
	var b0 int
	for b, o := range m.Owners {
		if o == "a" {
			b0 = b
			break
		}
	}
	m.Owners[b0] = "b"
	m.MovedAt = map[string]int64{strconv.Itoa(b0): 100}
	again, changed := Assign(m, []string{"b", "a"}, 16, 2048)
	if changed || again.Owners[b0] != "b" || again.MovedAt[strconv.Itoa(b0)] != 100 {
		t.Fatalf("rebalanced owner not kept: changed=%v owner=%s", changed, again.Owners[b0])
	}
}

func TestAssignResetsOnNewBucketCount(t *testing.T) {
	m, _ := Assign(nil, []string{"a", "b"}, 16, 2048)
	m2, changed := Assign(m, []string{"a", "b"}, 32, 2048)
	if !changed || len(m2.Owners) != 32 || m2.Version != 2 {
		t.Fatalf("changed=%v owners=%d version=%d", changed, len(m2.Owners), m2.Version)
	}
}

func TestEncodeDecode(t *testing.T) {
	m, _ := Assign(nil, []string{"a", "b"}, 8, 100)
	s, err := m.Encode()
	if err != nil {
		t.Fatal(err)
	}
	d, err := Decode(s)
	if err != nil || d.Buckets != 8 || d.PrefixBytes != 100 || d.Owners[3] != m.Owners[3] {
		t.Fatalf("round trip: %v %+v", err, d)
	}
	if _, err := Decode(`{"buckets":4,"owners":["a"]}`); err == nil {
		t.Fatal("malformed map accepted")
	}
}

var qcfg = Config{Trigger: QueueDelay, QueueDelayPercent: 200, KVCacheUsagePercent: 90, MinRequests: 20,
	MaxMoves: 4, Cooldown: 2 * time.Minute}

func TestOverloaded(t *testing.T) {
	cases := []struct {
		l    Load
		want bool
	}{
		{Load{OK: true, Requests: 100, MeanQueueSec: 0.5, MeanServiceSec: 0.2}, true},
		{Load{OK: true, Requests: 100, MeanQueueSec: 0.3, MeanServiceSec: 0.2}, false},
		{Load{OK: true, Requests: 5, MeanQueueSec: 5, MeanServiceSec: 0.2}, false}, // too few requests
		{Load{OK: false, Requests: 100, MeanQueueSec: 5, MeanServiceSec: 0.2}, false},
	}
	for i, c := range cases {
		if got := qcfg.Overloaded(c.l); got != c.want {
			t.Errorf("case %d: got %v", i, got)
		}
	}
	kv := qcfg
	kv.Trigger = KVCacheUsage
	if !kv.Overloaded(Load{OK: true, KVCacheUsage: 0.95}) || kv.Overloaded(Load{OK: true, KVCacheUsage: 0.5}) {
		t.Error("KV trigger")
	}
}

func TestRebalanceMovesFromHotToCool(t *testing.T) {
	m, _ := Assign(nil, []string{"a", "b"}, 32, 2048)
	before := m.Counts()
	v := m.Version
	loads := []Load{
		{Name: "a", OK: true, Requests: 100, MeanQueueSec: 1, MeanServiceSec: 0.2},
		{Name: "b", OK: true, Requests: 100, MeanQueueSec: 0, MeanServiceSec: 0.2},
	}
	now := time.Unix(1000, 0)
	moves := Rebalance(m, loads, qcfg, now, nil)
	if len(moves) != 4 {
		t.Fatalf("%d moves", len(moves))
	}
	for _, mv := range moves {
		if mv.From != "a" || mv.To != "b" || m.Owners[mv.Bucket] != "b" {
			t.Fatalf("bad move %+v", mv)
		}
	}
	if m.Moves != 4 || m.LastMove != now.Unix() {
		t.Fatalf("moves %d at %d", m.Moves, m.LastMove)
	}
	if kept, _ := Assign(m, []string{"a", "b"}, 32, 2048); kept.Moves != 4 {
		t.Fatalf("move count lost by Assign: %d", kept.Moves)
	}
	if m.Counts()["a"] != before["a"]-4 || m.Version != v+1 {
		t.Fatalf("counts %v version %d", m.Counts(), m.Version)
	}
	// the same replica stays hot: moved buckets are in cooldown, others move
	moves2 := Rebalance(m, loads, qcfg, now.Add(30*time.Second), nil)
	for _, mv := range moves2 {
		for _, old := range moves {
			if mv.Bucket == old.Bucket {
				t.Fatalf("bucket %d moved again within the cooldown", mv.Bucket)
			}
		}
	}
}

func TestRebalanceStopsAtTheFloor(t *testing.T) {
	m, _ := Assign(nil, []string{"a", "b"}, 32, 2048)
	c := qcfg
	c.MaxMoves = 100
	c.MinBuckets = 4
	Rebalance(m, []Load{
		{Name: "a", OK: true, Requests: 100, MeanQueueSec: 1, MeanServiceSec: 0.2},
		{Name: "b", OK: true, Requests: 100, MeanServiceSec: 0.2},
	}, c, time.Unix(0, 0), nil)
	if got := m.Counts()["a"]; got != 4 {
		t.Fatalf("hot replica kept %d buckets, want the floor of 4", got)
	}
}

var hotA = []Load{
	{Name: "a", OK: true, Requests: 100, MeanQueueSec: 1, MeanServiceSec: 0.2},
	{Name: "b", OK: true, Requests: 100, MeanServiceSec: 0.2},
}

// owned returns the buckets of m owned by o, in index order.
func owned(m *Map, o string) []int {
	var out []int
	for b, x := range m.Owners {
		if x == o {
			out = append(out, b)
		}
	}
	return out
}

func TestRebalanceByLoadMovesTheBusiestThatFits(t *testing.T) {
	m, _ := Assign(nil, []string{"a", "b"}, 32, 2048)
	as, bs := owned(m, "a"), owned(m, "b")
	load := map[int]float64{as[0]: 50, as[1]: 30, as[2]: 10, as[3]: 5, bs[0]: 10}
	// expected: a 95, b 10. Moving 50 leaves a 45, b 60: b would be busier, so 50 stays.
	// 30 moves (a 65, b 40), then 10 (a 55, b 50); 5 would leave b at 55 > a at 50: stays.
	c := qcfg
	c.MaxMoves = 10
	moves := Rebalance(m, hotA, c, time.Unix(0, 0), load)
	got := make([]int, 0, len(moves))
	for _, mv := range moves {
		if mv.From != "a" || mv.To != "b" {
			t.Fatalf("bad move %+v", mv)
		}
		got = append(got, mv.Bucket)
	}
	if len(got) != 2 || got[0] != as[1] || got[1] != as[2] {
		t.Fatalf("moved %v, want %v then %v", got, as[1], as[2])
	}
}

func TestRebalanceByLoadAndReuseMovesTheLeastReusedFirst(t *testing.T) {
	m, _ := Assign(nil, []string{"a", "b"}, 32, 2048)
	as, bs := owned(m, "a"), owned(m, "b")
	// on a: a hot context (90% of its prompt tokens from cache), a bucket of requests that share only a template
	// (false locality, 10%) and the busiest bucket (95%); room for one move
	load := map[int]float64{as[0]: 20, as[1]: 20, as[2]: 30, bs[0]: 10}
	c := qcfg
	c.MaxMoves = 1

	if moves := Rebalance(m, hotA, c, time.Unix(0, 0), load); len(moves) != 1 || moves[0].Bucket != as[2] {
		t.Fatalf("by load: %+v, want the busiest %d", moves, as[2])
	}

	m, _ = Assign(nil, []string{"a", "b"}, 32, 2048) // Rebalance applied the move above: start again
	c.Reuse = map[int]float64{as[0]: 0.9, as[1]: 0.1, as[2]: 0.95, bs[0]: 0.5}
	// load x (1 - reuse): as[0] 2, as[1] 18, as[2] 1.5: the false-locality bucket goes first
	if moves := Rebalance(m, hotA, c, time.Unix(0, 0), load); len(moves) != 1 || moves[0].Bucket != as[1] {
		t.Fatalf("by load and reuse: %+v, want the least reused %d", moves, as[1])
	}
}

func TestRebalanceByLoadLeavesADominantBucket(t *testing.T) {
	m, _ := Assign(nil, []string{"a", "b"}, 16, 2048)
	as := owned(m, "a")
	load := map[int]float64{as[0]: 100, as[1]: 1}
	c := qcfg
	c.MaxMoves = 10
	moves := Rebalance(m, hotA, c, time.Unix(0, 0), load)
	// 100 cannot move (b would carry it all); the bucket with 1 request moves (a 100, b 1)
	if len(moves) != 1 || moves[0].Bucket != as[1] {
		t.Fatalf("moves %+v", moves)
	}
}

func TestRebalanceByLoadSpreadsOverTargets(t *testing.T) {
	m, _ := Assign(nil, []string{"a", "b", "c"}, 48, 2048)
	as := owned(m, "a")
	load := map[int]float64{as[0]: 40, as[1]: 40, as[2]: 20}
	loads := append([]Load{}, hotA...)
	loads = append(loads, Load{Name: "c", OK: true, Requests: 100, MeanServiceSec: 0.2})
	c := qcfg
	c.MaxMoves = 10
	moves := Rebalance(m, loads, c, time.Unix(0, 0), load)
	// a 100, b 0, c 0: 40 -> b (60/40/0), 40 -> c (20/40/40); 20 would leave its target at 60 > 0: stays
	if len(moves) != 2 || moves[0].To == moves[1].To {
		t.Fatalf("moves %+v", moves)
	}
}

func TestRebalanceNoMoves(t *testing.T) {
	m, _ := Assign(nil, []string{"a", "b"}, 8, 2048)
	calm := []Load{{Name: "a", OK: true, Requests: 100, MeanServiceSec: 0.2}, {Name: "b", OK: true, Requests: 100, MeanServiceSec: 0.2}}
	if mv := Rebalance(m, calm, qcfg, time.Unix(0, 0), nil); mv != nil {
		t.Fatalf("moves without a trigger: %+v", mv)
	}
	allHot := []Load{
		{Name: "a", OK: true, Requests: 100, MeanQueueSec: 1, MeanServiceSec: 0.2},
		{Name: "b", OK: true, Requests: 100, MeanQueueSec: 1, MeanServiceSec: 0.2},
	}
	if mv := Rebalance(m, allHot, qcfg, time.Unix(0, 0), nil); mv != nil {
		t.Fatalf("moves with every replica hot: %+v", mv)
	}
	// a hot replica with a single bucket keeps it
	one := &Map{Version: 1, Hash: HashName, Buckets: 2, PrefixBytes: 10, Owners: []string{"a", "b"}}
	if mv := Rebalance(one, []Load{allHot[0], calm[1]}, qcfg, time.Unix(0, 0), nil); mv != nil {
		t.Fatalf("last bucket moved: %+v", mv)
	}
}
