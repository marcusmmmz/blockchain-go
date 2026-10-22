package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"runtime"
	"syscall"
)

type peerFlags []string

func (p *peerFlags) String() string     { return fmt.Sprint([]string(*p)) }
func (p *peerFlags) Set(s string) error { *p = append(*p, s); return nil }
func main() {
	if err := run(); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}
func run() error {
	listen := flag.String("listen", "127.0.0.1:7331", "HTTP/WebSocket listen address")
	dir := flag.String("data-dir", "./blockchain-data", "exclusive local chain data directory")
	miner := flag.String("miner", "miner", "display label (1..32 UTF-8 bytes; not an authenticated wallet)")
	workers := flag.Int("workers", runtime.NumCPU(), "parallel mining workers")
	mine := flag.Bool("mine", true, "mine blocks; false runs a validating relay")
	bits := flag.Int("initial-bits", 22, "initial difficulty bits (1..32); MUST match across the network")
	var peers peerFlags
	flag.Var(&peers, "peer", "peer URL, e.g. wss://chain.example.com/p2p (repeatable)")
	flag.Parse()
	if flag.NArg() != 0 {
		return fmt.Errorf("unexpected positional arguments")
	}
	cfg := Config{InitialBits: *bits}
	if err := cfg.validate(); err != nil {
		return err
	}
	// Validate options before creating persistent state.
	opts := NodeOptions{Token: os.Getenv("BLOCKCHAIN_PEER_TOKEN"), Peers: peers, AccessID: os.Getenv("CF_ACCESS_CLIENT_ID"), AccessSecret: os.Getenv("CF_ACCESS_CLIENT_SECRET"), Miner: *miner, Workers: *workers, Mine: *mine}
	if _, err := newNode(newChain(cfg), opts); err != nil {
		return err
	}
	chain, err := openChain(*dir, cfg)
	if err != nil {
		return err
	}
	defer chain.Close()
	node, err := newNode(chain, opts)
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	log.Printf("chain=%s listen=%s miner=%q workers=%d mining=%v", cfg.ID(), listener.Addr(), *miner, *workers, *mine)
	return node.Run(ctx, listener)
}
