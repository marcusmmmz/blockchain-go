package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/gofrs/flock"
)

type storageError struct{ err error }

func (e *storageError) Error() string { return "chain storage failure: " + e.err.Error() }
func (e *storageError) Unwrap() error { return e.err }

type Chain struct {
	failure error
	closed  bool
	mu      sync.Mutex
	config  Config
	entries map[Hash]*entry
	tip     *entry
	changed chan struct{}
	log     *os.File
	lock    *flock.Flock
	now     func() time.Time
}

func newChain(config Config) *Chain {
	genesis := config.genesis()
	h := genesis.hash()
	e := &entry{block: genesis, hash: h, work: new(big.Int)}
	return &Chain{config: config, entries: map[Hash]*entry{h: e}, tip: e, changed: make(chan struct{}), now: time.Now}
}
func openChain(dir string, config Config) (_ *Chain, err error) {
	if err = config.validate(); err != nil {
		return nil, err
	}
	if err = os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	c := newChain(config)
	c.lock = flock.New(filepath.Join(dir, "LOCK"))
	ok, err := c.lock.TryLock()
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.New("data directory is already in use")
	}
	defer func() {
		if err != nil {
			c.Close()
		}
	}()
	meta := filepath.Join(dir, "chain-id")
	data, readErr := os.ReadFile(meta)
	if readErr == nil {
		if string(data) != config.ID().String()+"\n" {
			return nil, errors.New("data directory belongs to another chain")
		}
	} else if os.IsNotExist(readErr) {
		if err = os.WriteFile(meta, []byte(config.ID().String()+"\n"), 0600); err != nil {
			return nil, err
		}
	} else {
		return nil, readErr
	}
	c.log, err = os.OpenFile(filepath.Join(dir, "blocks.log"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	var offset int64
	for {
		var header [4]byte
		n, e := io.ReadFull(c.log, header[:])
		if e == io.EOF && n == 0 {
			break
		}
		if e == io.ErrUnexpectedEOF {
			if err = c.log.Truncate(offset); err != nil {
				return nil, err
			}
			break
		}
		if e != nil {
			return nil, e
		}
		size := binary.LittleEndian.Uint32(header[:])
		if size == 0 || size > maxBlockSize {
			return nil, fmt.Errorf("corrupt block record at %d", offset)
		}
		record := make([]byte, int(size)+sha256.Size)
		if _, e = io.ReadFull(c.log, record); e == io.EOF || e == io.ErrUnexpectedEOF {
			if err = c.log.Truncate(offset); err != nil {
				return nil, err
			}
			break
		} else if e != nil {
			return nil, e
		}
		sum := sha256.Sum256(record[:size])
		if !bytes.Equal(sum[:], record[size:]) {
			return nil, fmt.Errorf("block checksum mismatch at %d", offset)
		}
		var b Block
		if err = decodeJSON(record[:size], &b); err != nil {
			return nil, err
		}
		if _, _, err = c.addLocked(b, false); err != nil {
			return nil, fmt.Errorf("replay at %d: %w", offset, err)
		}
		offset += int64(4 + len(record))
	}
	_, err = c.log.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}
	return c, nil
}
func (c *Chain) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var err error
	c.closed = true
	if c.log != nil {
		err = c.log.Close()
		c.log = nil
	}
	if c.lock != nil {
		if e := c.lock.Unlock(); err == nil {
			err = e
		}
		c.lock = nil
	}
	return err
}
func (c *Chain) Add(b Block) (bool, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return false, false, errors.New("chain is closed")
	}
	if c.failure != nil {
		return false, false, c.failure
	}
	accepted, changed, err := c.addLocked(b, true)
	var failure *storageError
	if errors.As(err, &failure) {
		c.failure = err
	}
	return accepted, changed, err
}
func (c *Chain) addLocked(b Block, persist bool) (bool, bool, error) {
	h := b.hash()
	if _, ok := c.entries[h]; ok {
		return false, false, nil
	}
	p := c.entries[b.Header.Previous]
	if p == nil {
		return false, false, errParent
	}
	if err := validateBlock(b, p, uint64(c.now().Unix())); err != nil {
		return false, false, err
	}
	if persist && c.log != nil {
		data, err := json.Marshal(b)
		if err != nil {
			return false, false, err
		}
		sum := sha256.Sum256(data)
		record := make([]byte, 4+len(data)+32)
		binary.LittleEndian.PutUint32(record, uint32(len(data)))
		copy(record[4:], data)
		copy(record[4+len(data):], sum[:])
		offset, err := c.log.Seek(0, io.SeekCurrent)
		if err != nil {
			return false, false, &storageError{err}
		}
		if _, err = c.log.Write(record); err == nil {
			err = c.log.Sync()
		}
		if err != nil {
			// Stop future appends if rollback cannot be made durable.
			rollback := c.log.Truncate(offset)
			_, seekErr := c.log.Seek(offset, io.SeekStart)
			syncErr := c.log.Sync()
			if rollback != nil || seekErr != nil || syncErr != nil {
				c.log.Close()
				c.log = nil
				return false, false, &storageError{errors.New("block log failed and could not be recovered; restart node")}
			}
			return false, false, &storageError{err}
		}
	}
	e := &entry{block: b, hash: h, parent: p, height: p.height + 1, work: new(big.Int).Add(p.work, blockWork(b.Header.Target))}
	c.entries[h] = e
	changed := e.work.Cmp(c.tip.work) > 0
	if changed {
		c.tip = e
		close(c.changed)
		c.changed = make(chan struct{})
	}
	return true, changed, nil
}
func (c *Chain) snapshot() (*entry, <-chan struct{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.tip, c.changed
}
func (c *Chain) locator(start Hash) []Hash {
	c.mu.Lock()
	defer c.mu.Unlock()
	e := c.entries[start]
	if e == nil {
		e = c.tip
	}
	out := make([]Hash, 0, 64)
	step := uint64(1)
	for e != nil && len(out) < 63 {
		out = append(out, e.hash)
		if e.parent == nil {
			return out
		}
		for i := uint64(0); i < step && e.parent != nil; i++ {
			e = e.parent
		}
		if len(out) > 10 {
			step *= 2
		}
	}
	if out[len(out)-1] != c.config.genesis().hash() {
		out = append(out, c.config.genesis().hash())
	}
	return out
}
func (c *Chain) blocksAfter(locator []Hash) ([]Block, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	path := make([]*entry, c.tip.height+1)
	for e := c.tip; e != nil; e = e.parent {
		path[e.height] = e
	}
	canonical := make(map[Hash]uint64, len(path))
	for _, e := range path {
		canonical[e.hash] = e.height
	}
	var start uint64
	found := false
	for _, h := range locator {
		if height, ok := canonical[h]; ok {
			start = height + 1
			found = true
			break
		}
	}
	if !found {
		return nil, false
	}
	end := start + 128
	if end > uint64(len(path)) {
		end = uint64(len(path))
	}
	blocks := make([]Block, 0, end-start)
	for i := start; i < end; i++ {
		blocks = append(blocks, path[i].block)
	}
	return blocks, end < uint64(len(path))
}

type ChainStatus struct {
	Chain  Hash           `json:"chain"`
	Tip    Hash           `json:"tip"`
	Height uint64         `json:"height"`
	Work   string         `json:"work"`
	Wins   map[string]int `json:"wins"`
	Stale  int            `json:"stale"`
}

func (c *Chain) Status() ChainStatus {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := ChainStatus{Chain: c.config.ID(), Tip: c.tip.hash, Height: c.tip.height, Work: c.tip.work.String(), Wins: map[string]int{}, Stale: len(c.entries) - int(c.tip.height) - 1}
	for e := c.tip; e.parent != nil; e = e.parent {
		s.Wins[e.block.Coinbase.Miner]++
	}
	return s
}
func decodeJSON(data []byte, v any) error {
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		return err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return errors.New("trailing JSON data")
	}
	return nil
}
