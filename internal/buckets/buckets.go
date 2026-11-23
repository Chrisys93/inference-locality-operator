// Package buckets holds the prefix-bucket logic shared by the controller and the EPP scorer: the bucket key, the
// bucket map and its encoding, the initial assignment and the rebalancing decision. It has no Kubernetes
// dependencies.
package buckets

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"sort"
	"strconv"
)

// HashName names the key function, so a reader of a published map can check it matches its own.
const HashName = "fnv1a64-prefix-bytes"

// Key returns the bucket of a prompt: FNV-1a (64 bit) over its first prefixBytes bytes, modulo the bucket count.
func Key(prompt string, prefixBytes int, buckets int) int {
	if len(prompt) > prefixBytes {
		prompt = prompt[:prefixBytes]
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(prompt))
	return int(h.Sum64() % uint64(buckets))
}

// Map is the published bucket-to-replica map.
type Map struct {
	Version     int64    `json:"version"`
	Hash        string   `json:"hash"`
	Buckets     int      `json:"buckets"`
	PrefixBytes int      `json:"prefixBytes"`
	Owners      []string `json:"owners"` // Owners[b] is the pod name owning bucket b
	// MovedAt holds, per bucket moved by rebalancing, the Unix time of the move (for the cooldown).
	MovedAt map[string]int64 `json:"movedAt,omitempty"`
	// Moves counts the buckets moved by rebalancing since the map was first built; LastMove is the Unix time of
	// the latest. Kept here, with the map, so the count stays right whatever happens to status updates.
	Moves    int64 `json:"moves,omitempty"`
	LastMove int64 `json:"lastMove,omitempty"`
}

// Encode returns the map as JSON.
func (m *Map) Encode() (string, error) {
	b, err := json.Marshal(m)
	return string(b), err
}

// Decode parses a map and checks it is well formed.
func Decode(s string) (*Map, error) {
	var m Map
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		return nil, err
	}
	if m.Buckets <= 0 || len(m.Owners) != m.Buckets {
		return nil, fmt.Errorf("bucket map: %d owners for %d buckets", len(m.Owners), m.Buckets)
	}
	return &m, nil
}

// Counts returns the number of buckets each replica owns.
func (m *Map) Counts() map[string]int {
	c := map[string]int{}
	for _, o := range m.Owners {
		c[o]++
	}
	return c
}

// hrwOwner is the rendezvous-hashing winner for a bucket: the replica with the highest hash of (bucket, name).
// Adding or removing a replica only changes the buckets it wins or held.
func hrwOwner(bucket int, replicas []string) string {
	var best string
	var bestScore uint64
	for i, r := range replicas {
		h := fnv.New64a()
		_, _ = h.Write([]byte(strconv.Itoa(bucket) + "/" + r))
		if s := h.Sum64(); i == 0 || s > bestScore || (s == bestScore && r < best) {
			best, bestScore = r, s
		}
	}
	return best
}

// Assign returns the map for the given replicas, starting from prev (nil for none).
//
// With no usable previous map (none, or a different bucket count or prefix length) every bucket goes to its
// rendezvous-hashing winner. Otherwise buckets keep their owner, except that buckets of replicas that left go to
// their winner among the remaining replicas and a replica that joined takes the buckets it wins, so scaling moves
// about 1/n of the buckets and keeps earlier rebalancing elsewhere. changed reports whether any owner changed.
func Assign(prev *Map, replicas []string, buckets, prefixBytes int) (m *Map, changed bool) {
	names := append([]string(nil), replicas...)
	sort.Strings(names)
	m = &Map{Hash: HashName, Buckets: buckets, PrefixBytes: prefixBytes, Owners: make([]string, buckets)}
	if prev != nil {
		m.Moves, m.LastMove = prev.Moves, prev.LastMove
	}
	if len(names) == 0 {
		if prev != nil {
			m.Version = prev.Version + 1
		}
		return m, true
	}
	if prev == nil || prev.Buckets != buckets || prev.PrefixBytes != prefixBytes || prev.Hash != HashName {
		for b := range m.Owners {
			m.Owners[b] = hrwOwner(b, names)
		}
		if prev != nil {
			m.Version = prev.Version + 1
		} else {
			m.Version = 1
		}
		return m, true
	}

	present := map[string]bool{}
	for _, n := range names {
		present[n] = true
	}
	previous := map[string]bool{}
	for _, o := range prev.Owners {
		previous[o] = true
	}
	m.Version = prev.Version
	m.MovedAt = map[string]int64{}
	for b, o := range prev.Owners {
		w := hrwOwner(b, names)
		switch {
		case !present[o]: // owner left
			m.Owners[b] = w
		case !previous[w]: // the winner joined since the last map
			m.Owners[b] = w
		default:
			m.Owners[b] = o
			if t, ok := prev.MovedAt[strconv.Itoa(b)]; ok {
				m.MovedAt[strconv.Itoa(b)] = t
			}
		}
		if m.Owners[b] != o {
			changed = true
		}
	}
	if len(m.MovedAt) == 0 {
		m.MovedAt = nil
	}
	if changed {
		m.Version++
	}
	return m, changed
}
