package buckets

import (
	"cmp"
	"slices"
	"strconv"
	"time"
)

// Trigger selects the overload signal.
type Trigger string

const (
	// QueueDelay: mean queueing delay above QueueDelayPercent/100 x mean service time.
	QueueDelay Trigger = "QueueDelay"
	// KVCacheUsage: KV-cache use above KVCacheUsagePercent.
	KVCacheUsage Trigger = "KVCacheUsage"
)

// Load is what was observed of one replica over the last interval.
type Load struct {
	Name           string
	OK             bool // metrics were read
	Requests       int64
	MeanQueueSec   float64
	MeanServiceSec float64
	KVCacheUsage   float64 // 0..1
}

// Config holds the trigger and the limits on moves.
type Config struct {
	Trigger             Trigger
	QueueDelayPercent   int
	KVCacheUsagePercent int
	MinRequests         int64
	MaxMoves            int
	Cooldown            time.Duration
	// MinBuckets is the fewest buckets rebalancing leaves on a replica (at least 1).
	MinBuckets int
}

// Move is one bucket changing owner.
type Move struct {
	Bucket   int
	From, To string
}

// Overloaded reports whether the trigger fires for a replica.
func (c Config) Overloaded(l Load) bool {
	if !l.OK {
		return false
	}
	switch c.Trigger {
	case KVCacheUsage:
		return l.KVCacheUsage*100 > float64(c.KVCacheUsagePercent)
	default:
		return l.Requests >= c.MinRequests && l.MeanServiceSec > 0 &&
			l.MeanQueueSec > float64(c.QueueDelayPercent)/100*l.MeanServiceSec
	}
}

// pressure orders replicas from least to most loaded under the trigger's signal.
func (c Config) pressure(l Load) float64 {
	if c.Trigger == KVCacheUsage {
		return l.KVCacheUsage
	}
	if l.MeanServiceSec <= 0 {
		return 0
	}
	return l.MeanQueueSec / l.MeanServiceSec
}

// Rebalance decides which buckets move away from overloaded replicas, and applies the moves to m (version and
// MovedAt included).
//
// Common rules: only buckets out of their cooldown move; an overloaded replica keeps at least MinBuckets (and at
// least one) bucket; targets are replicas whose metrics were read and whose trigger did not fire; at most MaxMoves
// buckets move in total; the most pressured replica sheds first.
//
// bucketLoad, when known (requests per bucket in the interval, from the scorer), selects as ReStorEdge's workload
// orchestration does inside its queueing trigger: each replica's expected load is the sum of its buckets' loads;
// the overloaded replica's busiest buckets go first, each to the cool replica with the least expected load, and a
// bucket moves only if the target's expected load stays at or below the source's afterwards; expected loads are
// updated after every move. So one move can relieve a lot, and nothing moves that would just move the overload. A
// bucket busier than half the gap stays (splitting or replicating it is the remedy, not moving it). Buckets with
// no requests do not move. Without bucketLoad, buckets move in index order from a starting point that advances with
// the map version, to the cool replicas (least pressure first) in turn.
func Rebalance(m *Map, loads []Load, c Config, now time.Time, bucketLoad map[int]float64) []Move {
	if c.MaxMoves <= 0 || len(m.Owners) == 0 {
		return nil
	}
	var hot, cool []Load
	for _, l := range loads {
		if c.Overloaded(l) {
			hot = append(hot, l)
		} else if l.OK {
			cool = append(cool, l)
		}
	}
	if len(hot) == 0 || len(cool) == 0 {
		return nil
	}
	counts := m.Counts()
	slices.SortStableFunc(hot, func(a, b Load) int { return cmp.Compare(c.pressure(b), c.pressure(a)) })
	slices.SortStableFunc(cool, func(a, b Load) int {
		return cmp.Or(
			cmp.Compare(c.pressure(a), c.pressure(b)),
			cmp.Compare(counts[a.Name], counts[b.Name]),
			cmp.Compare(a.Name, b.Name),
		)
	})
	var moves []Move
	if bucketLoad != nil {
		moves = c.movesByLoad(m, hot, cool, counts, now, bucketLoad)
	} else {
		moves = c.movesInTurn(m, hot, cool, counts, now)
	}
	if len(moves) > 0 {
		if m.MovedAt == nil {
			m.MovedAt = map[string]int64{}
		}
		for _, mv := range moves {
			m.Owners[mv.Bucket] = mv.To
			m.MovedAt[strconv.Itoa(mv.Bucket)] = now.Unix()
		}
		m.Version++
		m.Moves += int64(len(moves))
		m.LastMove = now.Unix()
	}
	return moves
}

// movable lists a replica's buckets that are out of their cooldown, starting from a point that advances with the
// map version.
func (c Config) movable(m *Map, owner string, now time.Time) []int {
	n := len(m.Owners)
	start := int(m.Version) % n
	var owned []int
	for i := range n {
		b := (start + i) % n
		if m.Owners[b] != owner {
			continue
		}
		if at, ok := m.MovedAt[strconv.Itoa(b)]; ok && now.Sub(time.Unix(at, 0)) < c.Cooldown {
			continue
		}
		owned = append(owned, b)
	}
	return owned
}

func (c Config) movesInTurn(m *Map, hot, cool []Load, counts map[string]int, now time.Time) []Move {
	floor := max(c.MinBuckets, 1)
	var moves []Move
	t := 0
	for _, h := range hot {
		for _, b := range c.movable(m, h.Name, now) {
			if len(moves) >= c.MaxMoves || counts[h.Name] <= floor {
				break
			}
			to := cool[t%len(cool)].Name
			t++
			moves = append(moves, Move{Bucket: b, From: h.Name, To: to})
			counts[h.Name]--
			counts[to]++
		}
	}
	return moves
}

func (c Config) movesByLoad(m *Map, hot, cool []Load, counts map[string]int, now time.Time,
	bucketLoad map[int]float64) []Move {
	floor := max(c.MinBuckets, 1)
	expected := map[string]float64{}
	for b, o := range m.Owners {
		expected[o] += bucketLoad[b]
	}
	var moves []Move
	for _, h := range hot {
		owned := slices.DeleteFunc(c.movable(m, h.Name, now), func(b int) bool { return bucketLoad[b] <= 0 })
		slices.SortStableFunc(owned, func(a, b int) int {
			return cmp.Or(cmp.Compare(bucketLoad[b], bucketLoad[a]), cmp.Compare(a, b))
		})
		for _, b := range owned {
			if len(moves) >= c.MaxMoves || counts[h.Name] <= floor {
				break
			}
			to := cool[0].Name
			for _, l := range cool[1:] {
				if expected[l.Name] < expected[to] {
					to = l.Name
				}
			}
			load := bucketLoad[b]
			if expected[to]+load > expected[h.Name]-load {
				continue // the target would end up busier than the source: try a smaller bucket
			}
			moves = append(moves, Move{Bucket: b, From: h.Name, To: to})
			counts[h.Name]--
			counts[to]++
			expected[h.Name] -= load
			expected[to] += load
		}
	}
	return moves
}
