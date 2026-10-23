package test

import (
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// linkProxy is a TCP proxy standing in for ONE direction of the network
// link between two nodes (e.g. "node 1 calling node 2"). Fault-injection
// tests start nodes whose -peers point at these proxies instead of at each
// other, so the test can cut a link at will, simulating a network
// partition between live processes.
//
// This exists only in tests: the nodes have no idea a proxy is there, and
// nothing in node/ or rpc/ knows about fault injection. A partition is a
// real network failure from the nodes' point of view: their RPCs fail to
// connect or get cut off mid-call, exactly as on a broken network.
//
// While a link is blocked, the proxy accepts and immediately closes new
// connections (so the caller's RPC fails right away), and it has closed
// every connection that was open when the block started (so replies that
// were already in flight can't sneak through the partition either).
type linkProxy struct {
	ln net.Listener

	mu      sync.Mutex
	target  string // address of the node this link leads to
	blocked bool
	conns   map[net.Conn]struct{} // open connections, both sides of each pair
}

// newLinkProxy starts a proxy on a free localhost port, forwarding to
// target, and stops it when the test ends.
func newLinkProxy(t *testing.T, target string) *linkProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("starting link proxy: %v", err)
	}
	p := &linkProxy{ln: ln, target: target, conns: map[net.Conn]struct{}{}}
	go p.serve()
	t.Cleanup(p.close)
	return p
}

// addr is where callers should connect to reach target through this link.
func (p *linkProxy) addr() string { return p.ln.Addr().String() }

// setBlocked cuts (true) or restores (false) the link.
func (p *linkProxy) setBlocked(blocked bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.blocked = blocked
	if blocked {
		for c := range p.conns {
			c.Close()
		}
		p.conns = map[net.Conn]struct{}{}
	}
}

// setTarget points the link at a new address, e.g. when the node at its
// far end is restarted on a different port.
func (p *linkProxy) setTarget(target string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.target = target
}

func (p *linkProxy) close() {
	p.ln.Close()
	p.setBlocked(true) // closes every open connection
}

func (p *linkProxy) serve() {
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return // listener closed at the end of the test
		}
		go p.handle(c)
	}
}

// handle forwards one connection to the target, in both directions, until
// either side closes it or the link gets blocked.
func (p *linkProxy) handle(client net.Conn) {
	p.mu.Lock()
	blocked, target := p.blocked, p.target
	p.mu.Unlock()
	if blocked {
		client.Close()
		return
	}

	upstream, err := net.DialTimeout("tcp", target, time.Second)
	if err != nil {
		client.Close() // far end is down: the caller sees a failed RPC
		return
	}

	// Register both sides so setBlocked can cut them, re-checking the
	// block in case it started while we were dialing.
	p.mu.Lock()
	if p.blocked {
		p.mu.Unlock()
		client.Close()
		upstream.Close()
		return
	}
	p.conns[client] = struct{}{}
	p.conns[upstream] = struct{}{}
	p.mu.Unlock()

	var once sync.Once
	closeBoth := func() {
		once.Do(func() {
			client.Close()
			upstream.Close()
			p.mu.Lock()
			delete(p.conns, client)
			delete(p.conns, upstream)
			p.mu.Unlock()
		})
	}
	go func() {
		io.Copy(upstream, client)
		closeBoth()
	}()
	io.Copy(client, upstream)
	closeBoth()
}
