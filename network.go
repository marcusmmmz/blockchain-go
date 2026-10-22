package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/coder/websocket"
)

const maxMessageSize = 1 << 20
const maxPeers = 32

type message struct {
	Type    string  `json:"type"`
	Version int     `json:"version,omitempty"`
	Chain   Hash    `json:"chain,omitempty"`
	Session Hash    `json:"session,omitempty"`
	Tip     Hash    `json:"tip,omitempty"`
	Height  uint64  `json:"height,string,omitempty"`
	Locator []Hash  `json:"locator,omitempty"`
	Blocks  []Block `json:"blocks,omitempty"`
	Block   *Block  `json:"block,omitempty"`
	More    bool    `json:"more,omitempty"`
}
type peer struct {
	id       Hash
	conn     *websocket.Conn
	queue    chan message
	outbound bool
	cancel   context.CancelFunc
}
type NodeOptions struct {
	Peers        []string
	AccessID     string
	AccessSecret string
	Miner        string
	Workers      int
	Mine         bool
}
type Node struct {
	chain   *Chain
	options NodeOptions
	id      Hash
	mu      sync.Mutex
	peers   map[Hash]*peer
	slots   chan struct{}
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	hashes  atomic.Uint64
	rateMu  sync.Mutex
	rate    float64
}

func validPeerURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "/p2p" {
		return errors.New("peer must be a ws(s) URL ending in /p2p, without credentials or query parameters")
	}
	if u.Scheme == "wss" {
		return nil
	}
	if u.Scheme == "ws" {
		ip := net.ParseIP(u.Hostname())
		if u.Hostname() == "localhost" || (ip != nil && ip.IsLoopback()) {
			return nil
		}
	}
	return errors.New("remote peers require wss://; ws:// is allowed only for loopback development")
}
func newNode(c *Chain, o NodeOptions) (*Node, error) {
	if (o.AccessID == "") != (o.AccessSecret == "") {
		return nil, errors.New("set both CF_ACCESS_CLIENT_ID and CF_ACCESS_CLIENT_SECRET, or neither")
	}
	if o.Workers < 1 || o.Workers > 1024 {
		return nil, errors.New("workers must be between 1 and 1024")
	}
	if len(o.Peers) > maxPeers {
		return nil, errors.New("too many configured peers")
	}
	for _, p := range o.Peers {
		if err := validPeerURL(p); err != nil {
			return nil, err
		}
	}
	if o.Mine && (!utf8.ValidString(o.Miner) || len(o.Miner) == 0 || len(o.Miner) > 32) {
		return nil, errors.New("miner label must contain 1..32 UTF-8 bytes")
	}
	n := &Node{chain: c, options: o, peers: map[Hash]*peer{}, slots: make(chan struct{}, maxPeers)}
	if _, err := rand.Read(n.id[:]); err != nil {
		return nil, err
	}
	return n, nil
}
func (n *Node) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/p2p", func(w http.ResponseWriter, r *http.Request) {
		select {
		case n.slots <- struct{}{}:
			defer func() { <-n.slots }()
		default:
			http.Error(w, "peer limit reached", http.StatusServiceUnavailable)
			return
		}
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{CompressionMode: websocket.CompressionDisabled})
		if err != nil {
			return
		}
		n.session(n.ctx, conn, false)
	})
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(n.Status())
	})
	return mux
}

type NodeStatus struct {
	ChainStatus
	Peers    int     `json:"peers"`
	Hashes   uint64  `json:"hashes"`
	HashRate float64 `json:"hashes_per_second"`
}

func (n *Node) Status() NodeStatus {
	s := n.chain.Status()
	n.mu.Lock()
	count := len(n.peers)
	n.mu.Unlock()
	n.rateMu.Lock()
	rate := n.rate
	n.rateMu.Unlock()
	return NodeStatus{ChainStatus: s, Peers: count, Hashes: n.hashes.Load(), HashRate: rate}
}

// Run owns the listener until ctx is canceled. The caller closes the chain after Run returns.
func (n *Node) Run(ctx context.Context, l net.Listener) error {
	n.ctx, n.cancel = context.WithCancel(ctx)
	server := &http.Server{Handler: n.handler(), ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 << 10}
	errorsCh := make(chan error, 1)
	go func() { errorsCh <- server.Serve(l) }()
	for _, address := range n.options.Peers {
		n.wg.Add(1)
		go func(address string) { defer n.wg.Done(); n.dialLoop(address) }(address)
	}
	if n.options.Mine {
		n.wg.Add(1)
		go func() { defer n.wg.Done(); n.mine(n.ctx) }()
	}
	n.wg.Add(1)
	go func() { defer n.wg.Done(); n.report(n.ctx) }()
	var err error
	select {
	case <-n.ctx.Done():
	case err = <-errorsCh:
	}
	n.cancel()
	server.Close()
	n.mu.Lock()
	for _, p := range n.peers {
		p.cancel()
		p.conn.CloseNow()
	}
	n.mu.Unlock()
	n.wg.Wait()
	n.chain.mu.Lock()
	failure := n.chain.failure
	n.chain.mu.Unlock()
	if failure != nil {
		return failure
	}
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
func (n *Node) report(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	var previous uint64
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			count := n.hashes.Load()
			n.rateMu.Lock()
			n.rate = float64(count-previous) / 5
			n.rateMu.Unlock()
			previous = count
			s := n.Status()
			log.Printf("height=%d tip=%s peers=%d hashes/s=%.0f wins=%v stale=%d", s.Height, s.Tip.String()[:12], s.Peers, s.HashRate, s.Wins, s.Stale)
		}
	}
}
func (n *Node) dialLoop(address string) {
	delay := time.Second
	for n.ctx.Err() == nil {
		select {
		case n.slots <- struct{}{}:
		case <-n.ctx.Done():
			return
		}
		h := http.Header{}
		if strings.HasPrefix(address, "wss://") && n.options.AccessID != "" {
			h.Set("CF-Access-Client-Id", n.options.AccessID)
			h.Set("CF-Access-Client-Secret", n.options.AccessSecret)
		}
		ctx, cancel := context.WithTimeout(n.ctx, 10*time.Second)
		client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("peer redirects are disabled") }}
		conn, response, err := websocket.Dial(ctx, address, &websocket.DialOptions{HTTPHeader: h, HTTPClient: client, CompressionMode: websocket.CompressionDisabled})
		cancel()
		if err == nil {
			delay = time.Second
			n.session(n.ctx, conn, true)
		} else if n.ctx.Err() == nil {
			// Avoid logging addresses/headers or upstream error bodies, which may contain secrets.
			status := 0
			if response != nil {
				status = response.StatusCode
			}
			log.Printf("peer connection failed (HTTP status %d); retrying", status)
		}
		<-n.slots
		var jitter [1]byte
		rand.Read(jitter[:])
		timer := time.NewTimer(delay + time.Duration(jitter[0])*time.Millisecond)
		select {
		case <-n.ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		if delay < 30*time.Second {
			delay *= 2
			if delay > 30*time.Second {
				delay = 30 * time.Second
			}
		}
	}
}
func writeMessage(ctx context.Context, c *websocket.Conn, m message) error {
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	if len(b) > maxMessageSize {
		return errors.New("outbound message too large")
	}
	deadline, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return c.Write(deadline, websocket.MessageText, b)
}
func readMessage(ctx context.Context, c *websocket.Conn) (message, error) {
	kind, b, err := c.Read(ctx)
	if err != nil {
		return message{}, err
	}
	if kind != websocket.MessageText {
		return message{}, errors.New("expected a text message")
	}
	var m message
	err = decodeJSON(b, &m)
	return m, err
}
func (n *Node) session(parent context.Context, conn *websocket.Conn, outbound bool) {
	defer conn.CloseNow()
	conn.SetReadLimit(maxMessageSize)
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	tip, _ := n.chain.snapshot()
	hello := message{Type: "hello", Version: protocolVersion, Chain: n.chain.config.ID(), Session: n.id, Tip: tip.hash, Height: tip.height}
	hs, done := context.WithTimeout(ctx, 10*time.Second)
	if err := writeMessage(hs, conn, hello); err != nil {
		done()
		return
	}
	remote, err := readMessage(hs, conn)
	done()
	if err != nil || remote.Type != "hello" || remote.Version != protocolVersion || remote.Chain != hello.Chain || remote.Session == n.id || remote.Session == (Hash{}) {
		return
	}
	p := &peer{id: remote.Session, conn: conn, queue: make(chan message, 64), outbound: outbound, cancel: cancel}
	n.mu.Lock()
	if ctx.Err() != nil {
		n.mu.Unlock()
		return
	}
	if old := n.peers[p.id]; old != nil {
		preferOutbound := n.id.String() < p.id.String()
		if old.outbound == preferOutbound || outbound != preferOutbound {
			n.mu.Unlock()
			return
		}
		old.cancel()
		old.conn.CloseNow()
	}
	n.peers[p.id] = p
	n.wg.Add(1)
	n.mu.Unlock()
	defer n.wg.Done()
	defer func() {
		n.mu.Lock()
		if n.peers[p.id] == p {
			delete(n.peers, p.id)
		}
		n.mu.Unlock()
	}()
	log.Printf("peer connected session=%s", p.id.String()[:12])
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		defer cancel()
		tick := time.NewTicker(15 * time.Second)
		defer tick.Stop()
		syncTick := time.NewTicker(5 * time.Second)
		defer syncTick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case m := <-p.queue:
				if writeMessage(ctx, conn, m) != nil {
					return
				}
			case <-tick.C:
				ping, stop := context.WithTimeout(ctx, 10*time.Second)
				err := conn.Ping(ping)
				stop()
				if err != nil {
					return
				}
			case <-syncTick.C:
				if writeMessage(ctx, conn, message{Type: "getblocks", Locator: n.chain.locator(Hash{})}) != nil {
					return
				}
			}
		}
	}()
	n.send(p, message{Type: "getblocks", Locator: n.chain.locator(Hash{})})
	window := time.Now()
	count := 0
	for ctx.Err() == nil {
		readCtx, stop := context.WithTimeout(ctx, 60*time.Second)
		m, e := readMessage(readCtx, conn)
		stop()
		if e != nil {
			break
		}
		if time.Since(window) > time.Second {
			window = time.Now()
			count = 0
		}
		count++
		if count > 256 {
			break
		}
		if e = n.handle(p, m); e != nil {
			log.Printf("peer protocol rejected session=%s: %v", p.id.String()[:12], e)
			break
		}
	}
	cancel()
	conn.CloseNow()
	<-writerDone
}
func (n *Node) send(p *peer, m message) {
	select {
	case p.queue <- m:
	default:
		p.cancel()
		p.conn.CloseNow()
	}
}
func (n *Node) broadcast(b Block, except *peer) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, p := range n.peers {
		if p != except {
			n.send(p, message{Type: "block", Block: &b})
		}
	}
}
func (n *Node) accept(b Block, source *peer) error {
	accepted, changed, err := n.chain.Add(b)
	if err != nil {
		var failure *storageError
		if errors.As(err, &failure) {
			log.Print(failure)
			if n.cancel != nil {
				n.cancel()
			}
		}
		return err
	}
	if accepted {
		n.broadcast(b, source)
	}
	if changed {
		s := n.chain.Status()
		log.Printf("accepted height=%d miner=%q tip=%s", s.Height, b.Coinbase.Miner, s.Tip.String()[:12])
	}
	return nil
}
func (n *Node) handle(p *peer, m message) error {
	switch m.Type {
	case "getblocks":
		if len(m.Locator) == 0 || len(m.Locator) > 64 {
			return errors.New("invalid locator length")
		}
		blocks, more := n.chain.blocksAfter(m.Locator)
		n.send(p, message{Type: "blocks", Blocks: blocks, More: more})
	case "blocks":
		if len(m.Blocks) > 128 || (m.More && len(m.Blocks) == 0) {
			return errors.New("invalid block batch")
		}
		for _, b := range m.Blocks {
			if err := n.accept(b, p); err != nil {
				if errors.Is(err, errParent) {
					n.send(p, message{Type: "getblocks", Locator: n.chain.locator(Hash{})})
					return nil
				}
				if errors.Is(err, errFuture) {
					return nil
				}
				return err
			}
		}
		if m.More {
			last := m.Blocks[len(m.Blocks)-1].hash()
			n.send(p, message{Type: "getblocks", Locator: n.chain.locator(last)})
		}
	case "block":
		if m.Block == nil {
			return errors.New("missing block")
		}
		if err := n.accept(*m.Block, p); err != nil {
			if errors.Is(err, errParent) {
				n.send(p, message{Type: "getblocks", Locator: n.chain.locator(Hash{})})
				return nil
			}
			if errors.Is(err, errFuture) {
				return nil
			}
			return err
		}
	default:
		return fmt.Errorf("unknown message type")
	}
	return nil
}
