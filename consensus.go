package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"unicode/utf8"
)

const protocolVersion = 2
const targetSpacing = 10
const maxBlockSize = 64 << 10

var two256 = new(big.Int).Lsh(big.NewInt(1), 256)
var maxTarget = new(big.Int).Sub(new(big.Int).Set(two256), big.NewInt(1))
var errParent = errors.New("unknown parent")
var errFuture = errors.New("block timestamp is too far in the future")

type Hash [32]byte

func (h Hash) String() string               { return hex.EncodeToString(h[:]) }
func (h Hash) MarshalJSON() ([]byte, error) { return json.Marshal(h.String()) }
func (h *Hash) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	if len(s) != 64 {
		return errors.New("hash must contain 64 hex characters")
	}
	raw, err := hex.DecodeString(s)
	if err != nil {
		return err
	}
	copy(h[:], raw)
	return nil
}
func targetHash(n *big.Int) Hash { var h Hash; n.FillBytes(h[:]); return h }
func (h Hash) integer() *big.Int { return new(big.Int).SetBytes(h[:]) }

type Coinbase struct {
	Miner  string `json:"miner"`
	Tag    Hash   `json:"tag"`
	Reward uint64 `json:"reward,string"`
}

func (c Coinbase) hash() Hash {
	var b bytes.Buffer
	binary.Write(&b, binary.LittleEndian, uint16(len(c.Miner)))
	b.WriteString(c.Miner)
	b.Write(c.Tag[:])
	binary.Write(&b, binary.LittleEndian, c.Reward)
	return sha256.Sum256(b.Bytes())
}

type Header struct {
	Previous   Hash   `json:"previous"`
	Timestamp  uint64 `json:"timestamp,string"`
	Commitment Hash   `json:"commitment"`
	Target     Hash   `json:"target"`
	Nonce      uint64 `json:"nonce,string"`
}

func (h Header) bytes() []byte {
	b := make([]byte, 112)
	copy(b, h.Previous[:])
	binary.LittleEndian.PutUint64(b[32:], h.Timestamp)
	copy(b[40:], h.Commitment[:])
	copy(b[72:], h.Target[:])
	binary.LittleEndian.PutUint64(b[104:], h.Nonce)
	return b
}
func (h Header) hash() Hash { return sha256.Sum256(h.bytes()) }

type Block struct {
	Header   Header   `json:"header"`
	Coinbase Coinbase `json:"coinbase"`
}

func (b Block) hash() Hash { return b.Header.hash() }

type Config struct {
	InitialBits int `json:"initial_bits"`
}

func (c Config) validate() error {
	if c.InitialBits < 1 || c.InitialBits > 32 {
		return errors.New("initial-bits must be between 1 and 32")
	}
	return nil
}
func (c Config) initialTarget() Hash {
	return targetHash(new(big.Int).Lsh(big.NewInt(1), uint(256-c.InitialBits)))
}
func (c Config) genesis() Block {
	cb := Coinbase{Miner: "genesis", Reward: 0}
	return Block{Header: Header{Timestamp: 1700000000, Commitment: cb.hash(), Target: c.initialTarget()}, Coinbase: cb}
}
func (c Config) ID() Hash {
	// Version commits to encoding, timestamp, reward, fork-choice and difficulty rules.
	return sha256.Sum256([]byte(fmt.Sprintf("blockchain-go/v%d/spacing=%d/daa=per-block-difficulty-1pct/genesis=%s", protocolVersion, targetSpacing, c.genesis().hash())))
}

type entry struct {
	block  Block
	hash   Hash
	parent *entry
	height uint64
	work   *big.Int
}

func medianTime(p *entry) uint64 {
	var times []uint64
	for i := 0; p != nil && i < 11; i++ {
		times = append(times, p.block.Header.Timestamp)
		p = p.parent
	}
	sort.Slice(times, func(i, j int) bool { return times[i] < times[j] })
	return times[len(times)/2]
}

// expectedTarget uses only the last completed interval on this branch. Its
// adjustment applies to the next block, whose target must be known before mining.
func expectedTarget(p *entry) Hash {
	if p.parent == nil {
		return p.block.Header.Target
	}
	previousTime := p.parent.block.Header.Timestamp
	currentTime := p.block.Header.Timestamp
	t := p.block.Header.Target.integer()
	switch {
	case currentTime <= previousTime || currentTime-previousTime < targetSpacing:
		// Difficulty is inversely proportional to target: D * 1.01 => T / 1.01.
		t.Mul(t, big.NewInt(100))
		t.Div(t, big.NewInt(101))
	case currentTime-previousTime > targetSpacing:
		// D * 0.99 => T / 0.99, rather than simply adding 1% to the target.
		t.Mul(t, big.NewInt(100))
		t.Div(t, big.NewInt(99))
	default:
		return p.block.Header.Target
	}
	if t.Sign() < 1 {
		t.SetInt64(1)
	}
	if t.Cmp(maxTarget) > 0 {
		t.Set(maxTarget)
	}
	return targetHash(t)
}

func validateBlock(b Block, p *entry, now uint64) error {
	if b.Header.Previous != p.hash {
		return errParent
	}
	if !utf8.ValidString(b.Coinbase.Miner) || len(b.Coinbase.Miner) == 0 || len(b.Coinbase.Miner) > 32 {
		return errors.New("miner label must be 1..32 UTF-8 bytes")
	}
	if b.Coinbase.Reward != 1 || b.Header.Commitment != b.Coinbase.hash() {
		return errors.New("invalid coinbase")
	}
	if b.Header.Target != expectedTarget(p) {
		return errors.New("incorrect target")
	}
	if b.Header.Timestamp <= medianTime(p) {
		return errors.New("timestamp must exceed median ancestor time")
	}
	if b.Header.Timestamp > now+120 {
		return errFuture
	}
	if b.hash().integer().Cmp(b.Header.Target.integer()) >= 0 {
		return errors.New("invalid proof of work")
	}
	return nil
}
func blockWork(target Hash) *big.Int {
	return new(big.Int).Div(new(big.Int).Set(two256), target.integer())
}
