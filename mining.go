package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"log"
	"sync"
	"time"
)

func candidate(p *entry, miner string, tag Hash, now uint64) Block {
	timestamp := now
	if timestamp <= medianTime(p) {
		timestamp = medianTime(p) + 1
	}
	cb := Coinbase{Miner: miner, Tag: tag, Reward: 1}
	return Block{Header: Header{Previous: p.hash, Timestamp: timestamp, Commitment: cb.hash(), Target: expectedTarget(p)}, Coinbase: cb}
}
func (n *Node) mine(ctx context.Context) {
	for ctx.Err() == nil {
		p, changed := n.chain.snapshot()
		var tag Hash
		if _, err := rand.Read(tag[:]); err != nil {
			log.Printf("mining randomness failed")
			return
		}
		b := candidate(p, n.options.Miner, tag, uint64(time.Now().Unix()))
		if b.Header.Timestamp > uint64(time.Now().Unix())+120 {
			timer := time.NewTimer(time.Second)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-changed:
				timer.Stop()
			case <-timer.C:
			}
			continue
		}
		job, stop := context.WithCancel(ctx)
		found := make(chan Block, 1)
		var wg sync.WaitGroup
		for i := 0; i < n.options.Workers; i++ {
			wg.Add(1)
			go func(worker int) { defer wg.Done(); n.search(job, b, worker, n.options.Workers, found) }(i)
		}
		refresh := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
		case <-changed:
		case <-refresh.C:
		case result := <-found:
			if err := n.accept(result, nil); err != nil {
				log.Printf("local candidate rejected: %v", err)
			}
		}
		refresh.Stop()
		stop()
		wg.Wait()
	}
}
func (n *Node) search(ctx context.Context, b Block, worker, workers int, found chan<- Block) {
	data := b.Header.bytes()
	nonce := uint64(worker)
	step := uint64(workers)
	count := uint64(0)
	defer func() { n.hashes.Add(count) }()
	for {
		if count%1024 == 0 {
			select {
			case <-ctx.Done():
				return
			default:
			}
			if count > 0 {
				n.hashes.Add(count)
				count = 0
			}
		}
		binary.LittleEndian.PutUint64(data[104:], nonce)
		hash := sha256.Sum256(data)
		count++
		if lessHash(hash, b.Header.Target) {
			b.Header.Nonce = nonce
			select {
			case found <- b:
			case <-ctx.Done():
			}
			return
		}
		if nonce > ^uint64(0)-step {
			return
		}
		nonce += step
	}
}
func lessHash(a, b Hash) bool {
	for i := range a {
		if a[i] != b[i] {
			return a[i] < b[i]
		}
	}
	return false
}
