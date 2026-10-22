package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestPersistenceRecoveryAndLock(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{InitialBits: 1}
	c, err := openChain(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if other, err := openChain(dir, cfg); err == nil {
		other.Close()
		t.Fatal("opened locked data dir")
	}
	p, _ := c.snapshot()
	p = addTest(t, c, child(t, p, "alice", uint64(time.Now().Unix())))
	want := p.hash
	if err = c.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "blocks.log")
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	before, _ := f.Stat()
	f.Write([]byte{255, 1})
	f.Close()
	c, err = openChain(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if s := c.Status(); s.Tip != want || s.Wins["alice"] != 1 {
		t.Fatal("replay changed chain")
	}
	after, _ := os.Stat(path)
	if after.Size() != before.Size() {
		t.Fatal("did not truncate partial tail")
	}
	p, _ = c.snapshot()
	addTest(t, c, child(t, p, "bob", uint64(time.Now().Unix())))
	c.Close()
	if wrong, err := openChain(dir, Config{InitialBits: 2}); err == nil {
		wrong.Close()
		t.Fatal("opened wrong chain")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[12] ^= 1
	if err = os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	if broken, err := openChain(dir, cfg); err == nil {
		broken.Close()
		t.Fatal("accepted corrupted log")
	}
}
func TestPersistenceRetainsSideBranches(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{InitialBits: 1}
	c, err := openChain(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	g, _ := c.snapshot()
	a := addTest(t, c, child(t, g, "a", g.block.Header.Timestamp+1))
	b := addTest(t, c, child(t, g, "b", g.block.Header.Timestamp+2))
	b = addTest(t, c, child(t, b, "b", b.block.Header.Timestamp+1))
	want := c.Status()
	c.Close()
	c, err = openChain(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if c.entries[a.hash] == nil || c.entries[b.hash] == nil || c.Status().Tip != want.Tip || c.Status().Stale != 1 {
		t.Fatal("lost side branch on replay")
	}
}
func TestPartialRecordRecovery(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{InitialBits: 1}
	c, err := openChain(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	c.Close()
	if err = os.WriteFile(filepath.Join(dir, "blocks.log"), []byte{10, 0, 0, 0, 1, 2, 3}, 0600); err != nil {
		t.Fatal(err)
	}
	c, err = openChain(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	info, _ := c.log.Stat()
	if info.Size() != 0 {
		t.Fatal("partial payload not removed")
	}
}
func TestStorageFailureDoesNotAcceptOrContinue(t *testing.T) {
	c, err := openChain(t.TempDir(), Config{InitialBits: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	p, _ := c.snapshot()
	b := child(t, p, "a", p.block.Header.Timestamp+1)
	c.log.Close()
	if _, _, err = c.Add(b); err == nil {
		t.Fatal("accepted without storage")
	}
	if c.Status().Height != 0 {
		t.Fatal("accepted failed write")
	}
	if _, _, err = c.Add(b); err == nil {
		t.Fatal("continued after storage failure")
	}
}
