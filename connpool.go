package connpool

import (
	"net"
	"sort"
	"sync"
	"time"
)

const (
	DefaultClaimTimeout = 10 * time.Minute
	TimeoutThreshold    = 1 * time.Second
)

type DialFunc func() (net.Conn, error)

type Pool struct {
	// MinSize: the pool will always attempt to maintain at least these many
	// connections.
	MinSize int
	// ClaimTimeout: connections will be removed from pool if unclaimed for
	// longer than ClaimTimeout.  The default ClaimTimeout is 10 minutes.
	ClaimTimeout time.Duration
	// Dial: specifies the function used to create new connections
	Dial DialFunc

	conns []*pooledConn
	mutex sync.Mutex
}

// Start starts the pool, filling it to the MinSize and maintaining the
// connections.
func (p *Pool) Start() {
	if p.ClaimTimeout == 0 {
		p.ClaimTimeout = DefaultClaimTimeout
	}
	p.conns = make([]*pooledConn, 0)
	p.maintain()
	go func() {
		// Periodically call maintain
		for {
			time.Sleep(1 * time.Second)
			p.maintain()
		}
	}()
}

func (p *Pool) Get() (net.Conn, error) {
	p.mutex.Lock()
	// Look for an unexpired pooled connection
	for i, conn := range p.conns {
		if conn.expires.After(time.Now()) {
			// Use pooled connection
			p.removeAt(i)
			p.doMaintain()
			p.mutex.Unlock()
			return conn, nil
		}
	}

	// No pooled conn, dial our own
	p.mutex.Unlock()
	return p.dial()
}

// maintain maintains the pool, making sure that connections that are about to
// time out are replaced with new connections and that connections are sorted
// based on which are timing out soonest.
func (p *Pool) maintain() {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.doMaintain()
}

func (p *Pool) doMaintain() {
	newConns := make([]*pooledConn, 0)
	expiresThreshold := time.Now().Add(-1 * TimeoutThreshold)
	for _, conn := range p.conns {
		if conn.expires.After(expiresThreshold) {
			// keep conn
			newConns = append(newConns, conn)
		}
	}
	sort.Sort(byExpiration(newConns))
	p.conns = newConns

	// Add connections to get pool up to the MinSize
	connsNeeded := p.MinSize - len(p.conns)
	for i := 0; i < connsNeeded; i++ {
		go func() {
			for {
				c, err := p.dial()
				if err == nil {
					p.add(c, false)
					return
				}
			}
		}()
	}
}

func (p *Pool) add(conn *pooledConn, maintain bool) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.conns = append(p.conns, conn)
	if maintain {
		p.doMaintain()
	}
}

func (p *Pool) removeAt(i int) {
	oldConns := p.conns
	p.conns = make([]*pooledConn, len(oldConns)-1)
	copy(p.conns, oldConns[:i])
	copy(p.conns[i:], oldConns[i+1:])
}

func (p *Pool) dial() (*pooledConn, error) {
	expires := time.Now().Add(p.ClaimTimeout)
	c, err := p.Dial()
	if err != nil {
		return nil, err
	} else {
		conn := &pooledConn{c, expires}
		return conn, nil
	}
}

type pooledConn struct {
	conn    net.Conn
	expires time.Time
}

func (c *pooledConn) Read(b []byte) (int, error) {
	return c.conn.Read(b)
}

// Write implements the method from io.Reader
func (c *pooledConn) Write(b []byte) (int, error) {
	return c.conn.Write(b)
}

func (c *pooledConn) Close() error {
	return c.conn.Close()
}

func (c *pooledConn) LocalAddr() net.Addr {
	return c.conn.LocalAddr()
}

func (c *pooledConn) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

func (c *pooledConn) SetDeadline(t time.Time) error {
	return c.conn.SetDeadline(t)
}

func (c *pooledConn) SetReadDeadline(t time.Time) error {
	return c.conn.SetReadDeadline(t)
}

func (c *pooledConn) SetWriteDeadline(t time.Time) error {
	return c.conn.SetWriteDeadline(t)
}

type byExpiration []*pooledConn

func (a byExpiration) Len() int           { return len(a) }
func (a byExpiration) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byExpiration) Less(i, j int) bool { return a[i].expires.Before(a[j].expires) }
