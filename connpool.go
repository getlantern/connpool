package connpool

import (
	"net"
	"sort"
	"sync"
	"time"

	"github.com/getlantern/golog"
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

	stopped           bool
	log               golog.Logger
	conns             []*pooledConn
	mutex             sync.Mutex
	maintainRequested chan interface{}
}

// Start starts the pool, filling it to the MinSize and maintaining the
// connections.
func (p *Pool) Start() {
	p.log = golog.LoggerFor("connpool")

	p.log.Trace("Starting connection pool")
	if p.ClaimTimeout == 0 {
		p.log.Tracef("Defaulting ClaimTimeout to %s", DefaultClaimTimeout)
		p.ClaimTimeout = DefaultClaimTimeout
	}

	p.conns = make([]*pooledConn, 0)
	p.maintainRequested = make(chan interface{}, p.MinSize*10)
	go func() {
		for {
			<-p.maintainRequested
			if p.MinSize > 0 {
				p.log.Trace("We're pooling stuff, call maintain")
				if p.maintain() {
					p.log.Trace("We've stopped, exiting maintain loop")
					return
				}
			}
		}
	}()
	if p.MinSize > 0 {
		p.log.Trace("We're pooling stuff, periodically request maintain")
		go func() {
			for {
				p.requestMaintain()
				time.Sleep(1 * time.Second)
			}
		}()
	}
}

func (p *Pool) Stop() {
	p.log.Trace("Stopping connection pool")
	p.mutex.Lock()
	p.stopped = true
	p.mutex.Unlock()
}

func (p *Pool) Get() (net.Conn, error) {
	p.log.Trace("Getting conn from pool")
	p.mutex.Lock()
	p.log.Trace("Looking for an unexpired pooled conn")
	// Look for an unexpired pooled connection
	for i, conn := range p.conns {
		if conn.expires.After(time.Now()) {
			p.log.Trace("Using pooled conn")
			p.removeAt(i)
			p.requestMaintain()
			p.mutex.Unlock()
			return conn, nil
		}
	}

	p.log.Trace("No pooled conn, dialing our own")
	p.mutex.Unlock()
	return p.dial()
}

func (p *Pool) requestMaintain() {
	select {
	case p.maintainRequested <- nil:
		p.log.Trace("Maintain request accepted")
	default:
		p.log.Trace("Maintain request channel full, ignore request")
	}
}

// maintain maintains the pool, making sure that connections that are about to
// time out are replaced with new connections and that connections are sorted
// based on which are timing out soonest.
func (p *Pool) maintain() (stopped bool) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	if !p.stopped {
		p.doMaintain()
	} else {
		p.log.Trace("Pool stopped, close any lingering connections")
		for _, conn := range p.conns {
			conn.Close()
		}
		p.conns = make([]*pooledConn, 0)
	}
	return p.stopped
}

func (p *Pool) doMaintain() {
	newConns := make([]*pooledConn, 0)
	expiresThreshold := time.Now().Add(-1 * TimeoutThreshold)
	for _, conn := range p.conns {
		if conn.expires.After(expiresThreshold) {
			p.log.Trace("Keeping unexpired conn")
			newConns = append(newConns, conn)
		} else {
			p.log.Trace("Closing expired conn")
			conn.Close()
		}
	}
	sort.Sort(byExpiration(newConns))
	p.conns = newConns

	connsNeeded := p.MinSize - len(p.conns)
	var wg sync.WaitGroup
	wg.Add(connsNeeded)
	p.log.Tracef("Adding %d connections to get pool up to the MinSize", connsNeeded)
	for i := 0; i < connsNeeded; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 3; j++ {
				if j > 0 {
					p.log.Trace("Dial failed, retrying")
				}
				c, err := p.dial()
				if err == nil {
					p.conns = append(p.conns, c)
					p.log.Trace("Dialed successfully")
					return
				} else {
					p.log.Tracef("Error dialing: %s", err)
				}
			}
		}()
	}
	wg.Wait()
	p.log.Trace("Done with maintain")
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
