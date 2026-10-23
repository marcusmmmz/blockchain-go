package main

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math/big"
	"testing"
	"time"
)

func testChain() *Chain {
	c := newChain(Config{InitialBits: 1})
	c.now = func() time.Time { return time.Unix(1800000000, 0) }
	return c
}
func solve(t *testing.T, b Block) Block {
	t.Helper()
	for i := uint64(0); i < 10000000; i++ {
		b.Header.Nonce = i
		if lessHash(b.hash(), b.Header.Target) {
			return b
		}
	}
	t.Fatal("test PoW budget exhausted")
	return Block{}
}
func child(t *testing.T, p *entry, miner string, ts uint64) Block {
	t.Helper()
	tag := sha256.Sum256([]byte(miner))
	return solve(t, candidate(p, miner, tag, ts))
}
func addTest(t *testing.T, c *Chain, b Block) *entry {
	t.Helper()
	if ok, _, err := c.Add(b); err != nil || !ok {
		t.Fatalf("add: %v accepted=%v", err, ok)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.entries[b.hash()]
}
func TestCanonicalEncoding(t *testing.T) {
	h := Header{Previous: Hash{1}, Timestamp: 2, Commitment: Hash{3}, Target: Hash{4}, Nonce: 5}
	want := make([]byte, 112)
	want[0] = 1
	want[32] = 2
	want[40] = 3
	want[72] = 4
	want[104] = 5
	if string(h.bytes()) != string(want) {
		t.Fatal("header encoding changed")
	}
	if h.hash() != sha256.Sum256(want) {
		t.Fatal("incorrect header hash")
	}
	cb := Coinbase{Miner: "a", Tag: Hash{7}, Reward: 1}
	raw := make([]byte, 43)
	raw[0] = 1
	raw[2] = 'a'
	raw[3] = 7
	binary.LittleEndian.PutUint64(raw[35:], 1)
	if cb.hash() != sha256.Sum256(raw) {
		t.Fatal("coinbase encoding changed")
	}
	cfg := Config{InitialBits: 22}
	// Golden vectors pin genesis, config identity, and encoding across future changes.
	if cfg.genesis().hash().String() != "987b84929d131a7865006bdff4a9f8f4fcad94db252c2a995ab1967061422f57" || cfg.ID().String() != "cc7af83f3c81b1c68e75130cc2f964f11125f4ada46e82ce607b08d4b9a5769e" {
		t.Fatal("genesis or chain identity changed")
	}
	bytes, err := json.Marshal(h)
	if err != nil {
		t.Fatal(err)
	}
	var round Header
	if err = decodeJSON(bytes, &round); err != nil || round != h {
		t.Fatal("wire roundtrip", err)
	}
	for _, bad := range []string{`"abc"`, `"` + hex.EncodeToString(make([]byte, 33)) + `"`, `42`} {
		var hash Hash
		if json.Unmarshal([]byte(bad), &hash) == nil {
			t.Fatal("accepted invalid hash")
		}
	}
	if cfg.ID() == (Config{InitialBits: 21}).ID() {
		t.Fatal("config not committed to chain ID")
	}
	if (Config{InitialBits: 0}).validate() == nil {
		t.Fatal("accepted invalid config")
	}
}
func TestBlockValidation(t *testing.T) {
	c := testChain()
	p, _ := c.snapshot()
	good := child(t, p, "alice", p.block.Header.Timestamp+1)
	cases := []struct {
		name   string
		change func(*Block)
		want   error
	}{
		{"parent", func(b *Block) { b.Header.Previous = Hash{1} }, errParent},
		{"reward", func(b *Block) { b.Coinbase.Reward = 2 }, nil},
		{"commitment", func(b *Block) { b.Header.Commitment = Hash{1} }, nil},
		{"label", func(b *Block) { b.Coinbase.Miner = ""; b.Header.Commitment = b.Coinbase.hash() }, nil},
		{"utf8", func(b *Block) { b.Coinbase.Miner = string([]byte{255}); b.Header.Commitment = b.Coinbase.hash() }, nil},
		{"target", func(b *Block) { b.Header.Target = Hash{} }, nil},
		{"median", func(b *Block) { b.Header.Timestamp = p.block.Header.Timestamp }, nil},
		{"future", func(b *Block) { b.Header.Timestamp = 1800000121 }, errFuture},
		{"pow", func(b *Block) {
			for lessHash(b.hash(), b.Header.Target) {
				b.Header.Nonce++
			}
		}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := good
			tc.change(&b)
			_, _, err := c.Add(b)
			if err == nil {
				t.Fatal("accepted invalid block")
			}
			if tc.want != nil && !errors.Is(err, tc.want) {
				t.Fatalf("want %v got %v", tc.want, err)
			}
		})
	}
	addTest(t, c, good)
	if ok, changed, err := c.Add(good); err != nil || ok || changed {
		t.Fatal("duplicate not idempotent")
	}
}
func syntheticBranch(last uint64, step uint64, target Hash) *entry {
	var p *entry
	for h := uint64(0); h <= last; h++ {
		p = &entry{height: h, parent: p, block: Block{Header: Header{Timestamp: 1700000000 + h*step, Target: target}}}
	}
	return p
}
func TestPerBlockDifficulty(t *testing.T) {
	// Exact integer vectors distinguish a 1% difficulty change from a 1% target change.
	cases := []struct {
		name              string
		previous, current uint64
		target, want      int64
	}{
		{"fast", 91, 100, 10100, 10000},
		{"slow", 89, 100, 9900, 10000},
		{"on-time", 90, 100, 10100, 10100},
		{"same-second", 100, 100, 10100, 10000},
		{"backward-time", 101, 100, 10100, 10000},
		{"very-slow", 1, 1000000000, 9900, 10000},
		{"fast-rounding", 91, 100, 100, 99},
		{"slow-rounding", 89, 100, 100, 101},
		{"minimum-target", 91, 100, 1, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, height := range []uint64{1, 2, 58, 59, 60, 119} {
				p := syntheticBranch(height, 10, targetHash(big.NewInt(tc.target)))
				p.block.Header.Timestamp = tc.current
				p.parent.block.Header.Timestamp = tc.previous
				if got := expectedTarget(p).integer(); got.Cmp(big.NewInt(tc.want)) != 0 {
					t.Fatalf("height %d: target %s, want %d", height, got, tc.want)
				}
			}
		})
	}
	p := syntheticBranch(1, 11, targetHash(maxTarget))
	if expectedTarget(p).integer().Cmp(maxTarget) != 0 {
		t.Fatal("maximum target not clamped")
	}
	g := syntheticBranch(0, 10, (Config{InitialBits: 4}).initialTarget())
	if expectedTarget(g) != g.block.Header.Target {
		t.Fatal("first mined block must use the initial target")
	}
}
func TestDifficultyIgnoresOlderIntervals(t *testing.T) {
	p := syntheticBranch(10, 9, (Config{InitialBits: 4}).initialTarget())
	want := expectedTarget(p)
	for ancestor := p.parent.parent; ancestor != nil; ancestor = ancestor.parent {
		ancestor.block.Header.Timestamp += 100000000
	}
	if got := expectedTarget(p); got != want {
		t.Fatal("older timestamps affected the two-block adjustment")
	}
}
func TestBlockRejectsUnadjustedTarget(t *testing.T) {
	c := testChain()
	g, _ := c.snapshot()
	p := addTest(t, c, child(t, g, "alice", g.block.Header.Timestamp+9))
	b := child(t, p, "alice", p.block.Header.Timestamp+9)
	b.Header.Target = p.block.Header.Target
	b = solve(t, b)
	if _, _, err := c.Add(b); err == nil {
		t.Fatal("accepted valid PoW with an unadjusted target")
	}
}

func TestForkChoiceAndTipNotification(t *testing.T) {
	c := testChain()
	g, notify := c.snapshot()
	a := addTest(t, c, child(t, g, "alice", g.block.Header.Timestamp+1))
	select {
	case <-notify:
	default:
		t.Fatal("tip change did not cancel job")
	}
	b := addTest(t, c, child(t, g, "bob", g.block.Header.Timestamp+2))
	if tip, _ := c.snapshot(); tip != a {
		t.Fatal("tie changed tip")
	}
	b2 := addTest(t, c, child(t, b, "bob", b.block.Header.Timestamp+1))
	if tip, _ := c.snapshot(); tip != b2 {
		t.Fatal("greater-work branch did not win")
	}
	s := c.Status()
	if s.Wins["bob"] != 2 || s.Wins["alice"] != 0 || s.Stale != 1 {
		t.Fatalf("bad reorg standings: %+v", s)
	}
}
func TestShorterHarderBranchWins(t *testing.T) {
	c := testChain()
	g, _ := c.snapshot()
	slow, fast := g, g
	// Both forks share genesis, but retarget using their own timestamps.
	for i := uint64(1); i <= 59; i++ {
		fast = addTest(t, c, child(t, fast, "fast", g.block.Header.Timestamp+i))
		slow = addTest(t, c, child(t, slow, "slow", g.block.Header.Timestamp+i*10))
	}
	for i := uint64(60); i <= 62; i++ {
		slow = addTest(t, c, child(t, slow, "slow", g.block.Header.Timestamp+i*10))
	}
	fast = addTest(t, c, child(t, fast, "fast", g.block.Header.Timestamp+60))
	if fast.height >= slow.height || fast.work.Cmp(slow.work) <= 0 {
		t.Fatal("invalid test branches")
	}
	if tip, _ := c.snapshot(); tip != fast {
		t.Fatal("did not prefer shorter harder branch")
	}
}

func TestFutureBlockCanBeRetried(t *testing.T) {
	c := testChain()
	p, _ := c.snapshot()
	b := child(t, p, "alice", 1800000121)
	if _, _, err := c.Add(b); !errors.Is(err, errFuture) {
		t.Fatal("did not reject future block", err)
	}
	c.now = func() time.Time { return time.Unix(1800000001, 0) }
	if _, _, err := c.Add(b); err != nil {
		t.Fatal("future rejection permanently poisoned block", err)
	}
}
