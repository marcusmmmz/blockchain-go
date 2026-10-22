package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

const testToken = "friends-experiment-test-token"

type runningNode struct {
	n    *Node
	url  string
	stop func()
}

func startTestNode(t *testing.T, c *Chain, peers ...string) runningNode {
	t.Helper()
	n, err := newNode(c, NodeOptions{Token: testToken, Peers: peers, Workers: 1, Miner: "test"})
	if err != nil {
		t.Fatal(err)
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- n.Run(ctx, l) }()
	base := "http://" + l.Addr().String()
	waitFor(t, func() bool {
		req, _ := http.NewRequest("GET", base+"/status", nil)
		req.Header.Set("Authorization", "Bearer "+testToken)
		res, e := http.DefaultClient.Do(req)
		if e != nil {
			return false
		}
		res.Body.Close()
		return res.StatusCode == 200
	})
	stopped := false
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		select {
		case e := <-done:
			if e != nil {
				t.Error(e)
			}
		case <-time.After(5 * time.Second):
			t.Error("node shutdown timed out")
		}
	}
	t.Cleanup(stop)
	return runningNode{n: n, url: base, stop: stop}
}
func (r runningNode) peerURL() string { return "ws" + strings.TrimPrefix(r.url, "http") + "/p2p" }
func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(12 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition did not become true")
}
func connectProtocol(t *testing.T, r runningNode, version int, chain Hash) (*websocket.Conn, context.Context) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	conn, _, err := websocket.Dial(ctx, r.peerURL(), &websocket.DialOptions{HTTPHeader: http.Header{"Authorization": []string{"Bearer " + testToken}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.CloseNow() })
	remote, err := readMessage(ctx, conn)
	if err != nil || remote.Type != "hello" {
		t.Fatal("missing server hello", err)
	}
	var id Hash
	rand.Read(id[:])
	if err = writeMessage(ctx, conn, message{Type: "hello", Version: version, Chain: chain, Session: id}); err != nil {
		t.Fatal(err)
	}
	return conn, ctx
}
func TestUpgradeAuthenticationAndPeerURLValidation(t *testing.T) {
	r := startTestNode(t, testChain())
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for _, token := range []string{"", "wrong"} {
		conn, res, err := websocket.Dial(ctx, r.peerURL(), &websocket.DialOptions{HTTPHeader: http.Header{"Authorization": []string{"Bearer " + token}}})
		if conn != nil {
			conn.CloseNow()
		}
		if err == nil || res == nil || res.StatusCode != 401 {
			t.Fatal("unauthenticated upgrade accepted")
		}
	}
	for _, raw := range []string{"tcp://host:1/p2p", "ws://example.com/p2p", "wss://user:password@example.com/p2p", "wss://example.com/p2p?token=secret", "https://example.com/p2p"} {
		if validPeerURL(raw) == nil {
			t.Fatalf("accepted %s", raw)
		}
	}
	for _, raw := range []string{"ws://127.0.0.1:7331/p2p", "wss://chain.example.com/p2p"} {
		if err := validPeerURL(raw); err != nil {
			t.Fatal(err)
		}
	}
}
func TestProtocolRejectsWrongNetworkAndVersion(t *testing.T) {
	for _, tc := range []struct {
		version int
		chain   Hash
	}{{2, (Config{InitialBits: 1}).ID()}, {1, (Config{InitialBits: 2}).ID()}} {
		r := startTestNode(t, testChain())
		conn, ctx := connectProtocol(t, r, tc.version, tc.chain)
		if _, err := readMessage(ctx, conn); err == nil {
			t.Fatal("accepted incompatible peer")
		}
	}
}
func TestProtocolLimits(t *testing.T) {
	cases := []struct {
		name string
		data string
	}{
		{"malformed", `{"type":`},
		{"unknown-field", `{"type":"getblocks","unexpected":1}`},
		{"empty-locator", `{"type":"getblocks"}`},
		{"oversized", strings.Repeat("x", maxMessageSize+1)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := startTestNode(t, testChain())
			conn, ctx := connectProtocol(t, r, protocolVersion, r.n.chain.config.ID())
			if _, err := readMessage(ctx, conn); err != nil {
				t.Fatal(err)
			} // initial locator request
			_ = conn.Write(ctx, websocket.MessageText, []byte(tc.data))
			if _, err := readMessage(ctx, conn); err == nil {
				t.Fatal("accepted invalid message")
			}
		})
	}
}
func TestRelayLateJoinReorgAndReconnect(t *testing.T) {
	a := startTestNode(t, testChain())
	b := startTestNode(t, testChain(), a.peerURL())
	waitFor(t, func() bool { return a.n.Status().Peers == 1 && b.n.Status().Peers == 1 })
	g, _ := b.n.chain.snapshot()
	first := child(t, g, "bob", g.block.Header.Timestamp+1)
	if err := b.n.accept(first, nil); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return a.n.chain.Status().Tip == first.hash() })
	c := startTestNode(t, testChain(), a.peerURL())
	waitFor(t, func() bool { return c.n.chain.Status().Tip == first.hash() })
	// Extend a competing branch at C. A relays it to B; B must reorganize.
	g = lookup(c.n.chain, c.n.chain.config.genesis().hash())
	alt := child(t, g, "carol", g.block.Header.Timestamp+2)
	if err := c.n.accept(alt, nil); err != nil {
		t.Fatal(err)
	}
	c.n.chain.mu.Lock()
	parent := c.n.chain.entries[alt.hash()]
	c.n.chain.mu.Unlock()
	alt2 := child(t, parent, "carol", g.block.Header.Timestamp+3)
	if err := c.n.accept(alt2, nil); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return a.n.chain.Status().Tip == alt2.hash() && b.n.chain.Status().Tip == alt2.hash() })
	if s := b.n.chain.Status(); s.Wins["bob"] != 0 || s.Wins["carol"] != 2 {
		t.Fatal("standings did not reorganize")
	}
	// Force all host connections down; configured outbound peers must reconnect.
	a.n.mu.Lock()
	for _, p := range a.n.peers {
		p.conn.CloseNow()
	}
	a.n.mu.Unlock()
	waitFor(t, func() bool { return a.n.Status().Peers == 2 })
	if err := a.n.accept(child(t, lookup(a.n.chain, alt2.hash()), "alice", g.block.Header.Timestamp+4), nil); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		return a.n.chain.Status().Tip == b.n.chain.Status().Tip && a.n.chain.Status().Tip == c.n.chain.Status().Tip
	})
}
func TestPaginatedSync(t *testing.T) {
	source := testChain()
	p, _ := source.snapshot()
	for i := 0; i < 260; i++ {
		p = addTest(t, source, child(t, p, "source", p.block.Header.Timestamp+10))
	}
	a := startTestNode(t, source)
	b := startTestNode(t, testChain(), a.peerURL())
	waitFor(t, func() bool { return b.n.chain.Status().Tip == source.Status().Tip })
	if b.n.chain.Status().Height != 260 {
		t.Fatal("missing sync batches")
	}
}
func TestWebSocketThroughTLSReverseProxy(t *testing.T) {
	origin := startTestNode(t, testChain())
	u, _ := url.Parse(origin.url)
	proxy := httptest.NewTLSServer(httputil.NewSingleHostReverseProxy(u))
	defer proxy.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, "wss"+strings.TrimPrefix(proxy.URL, "https")+"/p2p", &websocket.DialOptions{HTTPClient: proxy.Client(), HTTPHeader: http.Header{"Authorization": []string{"Bearer " + testToken}}})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()
	hello, err := readMessage(ctx, conn)
	if err != nil || hello.Type != "hello" {
		t.Fatal("tunneled hello failed", err)
	}
	var session Hash
	rand.Read(session[:])
	if err = writeMessage(ctx, conn, message{Type: "hello", Version: protocolVersion, Chain: hello.Chain, Session: session}); err != nil {
		t.Fatal(err)
	}
	if m, err := readMessage(ctx, conn); err != nil || m.Type != "getblocks" {
		t.Fatal("tunneled sync failed", err)
	}
	readDone := make(chan struct{})
	go func() { defer close(readDone); conn.Read(ctx) }()
	if err = conn.Ping(ctx); err != nil {
		t.Fatal("tunneled ping failed", err)
	}
	cancel()
	<-readDone
}
func TestMiningPartitionsCancellationAndRemoteTip(t *testing.T) {
	c := testChain()
	p, _ := c.snapshot()
	n, _ := newNode(c, NodeOptions{Token: testToken, Workers: 2, Miner: "alice"})
	b := candidate(p, "alice", Hash{1}, p.block.Header.Timestamp+1)
	for i := 0; i < 2; i++ {
		found := make(chan Block, 1)
		n.search(context.Background(), b, i, 2, found)
		result := <-found
		if result.Header.Nonce%2 != uint64(i) {
			t.Fatal("workers overlap nonce sequences")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	b.Header.Target = Hash{}
	go func() { n.search(ctx, b, 0, 1, make(chan Block, 1)); close(done) }()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("worker ignored cancellation")
	}
	// An actual mining loop must follow a newly received tip and keep producing.
	c = newChain(Config{InitialBits: 18})
	n, _ = newNode(c, NodeOptions{Token: testToken, Workers: 1, Miner: "alice"})
	r := startTestNode(t, c)
	mineCtx, stop := context.WithCancel(context.Background())
	miningDone := make(chan struct{})
	go func() { n.mine(mineCtx); close(miningDone) }()
	defer func() { stop(); <-miningDone }()
	waitFor(t, func() bool { return c.Status().Height >= 1 })
	// Hold chain ownership while constructing and accepting a remote extension.
	// This controls the race without relying on one miner winning a random interval.
	c.mu.Lock()
	tip := c.tip
	remote := child(t, tip, "bob", uint64(time.Now().Unix()))
	_, changed, err := c.addLocked(remote, false)
	c.mu.Unlock()
	if err != nil || !changed {
		t.Fatalf("remote tip not accepted: %v", err)
	}
	waitFor(t, func() bool { s := c.Status(); return s.Height > tip.height+1 && s.Wins["bob"] == 1 })
	_ = r
}
func TestStatusRequiresToken(t *testing.T) {
	r := startTestNode(t, testChain())
	res, err := http.Get(r.url + "/status")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	io.Copy(io.Discard, res.Body)
	if res.StatusCode != 401 {
		t.Fatal("status must require token")
	}
	req, _ := http.NewRequest("GET", r.url+"/status", nil)
	req.Header.Set("Authorization", "Bearer "+testToken)
	res, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	var s NodeStatus
	if err = json.NewDecoder(res.Body).Decode(&s); err != nil {
		t.Fatal(err)
	}
	if s.Chain != r.n.chain.config.ID() {
		t.Fatal(fmt.Sprint(s))
	}
}

func lookup(c *Chain, h Hash) *entry { c.mu.Lock(); defer c.mu.Unlock(); return c.entries[h] }

func TestUnknownParentRecoveredBySynchronization(t *testing.T) {
	a := startTestNode(t, testChain())
	b := startTestNode(t, testChain(), a.peerURL())
	waitFor(t, func() bool { return a.n.Status().Peers == 1 })
	g, _ := a.n.chain.snapshot()
	first := child(t, g, "alice", g.block.Header.Timestamp+1)
	parent := addTest(t, a.n.chain, first) // Intentionally do not announce this parent.
	second := child(t, parent, "alice", g.block.Header.Timestamp+2)
	if err := a.n.accept(second, nil); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return b.n.chain.Status().Tip == second.hash() })
}
func TestPeerIdentityDeduplicationAndSelfConnection(t *testing.T) {
	a := startTestNode(t, testChain())
	b := startTestNode(t, testChain(), a.peerURL(), a.peerURL())
	waitFor(t, func() bool { return a.n.Status().Peers == 1 && b.n.Status().Peers == 1 })
	conn, ctx := connectProtocol(t, a, protocolVersion, a.n.chain.config.ID())
	if _, err := readMessage(ctx, conn); err != nil {
		t.Fatal(err)
	}
	// A second hello after negotiation is invalid, including attempts to self-identify.
	if err := writeMessage(ctx, conn, message{Type: "hello", Version: protocolVersion, Chain: a.n.chain.config.ID(), Session: a.n.id}); err != nil {
		t.Fatal(err)
	}
	if _, err := readMessage(ctx, conn); err == nil {
		t.Fatal("accepted repeated hello")
	}
	// Verify self identity is rejected in the initial hello too.
	ctx2, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	self, _, err := websocket.Dial(ctx2, a.peerURL(), &websocket.DialOptions{HTTPHeader: http.Header{"Authorization": []string{"Bearer " + testToken}}})
	if err != nil {
		t.Fatal(err)
	}
	defer self.CloseNow()
	if _, err = readMessage(ctx2, self); err != nil {
		t.Fatal(err)
	}
	if err = writeMessage(ctx2, self, message{Type: "hello", Version: protocolVersion, Chain: a.n.chain.config.ID(), Session: a.n.id}); err != nil {
		t.Fatal(err)
	}
	if _, err = readMessage(ctx2, self); err == nil {
		t.Fatal("accepted self connection")
	}
}
