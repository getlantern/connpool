package connpool

import (
	"log"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/getlantern/idletiming"
)

const (
	DefaultIdleTimeout = 10 * time.Minute
	TimeoutThreshold   = 1 * time.Second
)

type DialFunc func() (net.Conn, error)

type Pool struct {
	// MinSize: the pool will always attempt to maintain at least these many
	// connections.
	MinSize int
	// IdleTimeout: connections will be removed from pool if idle for longer
	// than IdleTimeout.  The default IdleTimeout is 10 minutes.
	IdleTimeout time.Duration
	// Dial: specifies the function used to create new connections
	Dial DialFunc

	conns []*pooledConn
	mutex sync.Mutex
}

// Start starts the pool, filling it to the MinSize and maintaining the
// connections.
func (p *Pool) Start() {
	log.Println("Starting connection pool")
	if p.IdleTimeout == 0 {
		p.IdleTimeout = DefaultIdleTimeout
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
	for i, conn := range p.conns {
		if conn.conn.TimesOutIn() > TimeoutThreshold {
			log.Println("Using pooled connection")
			p.removeAt(i)
			p.doMaintain()
			p.mutex.Unlock()
			return conn, nil
		}
	}

	// No pooled conn, dial our own
	p.mutex.Unlock()
	log.Println("Using new connection")
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
	for _, conn := range p.conns {
		if conn.conn.TimesOutIn() > TimeoutThreshold {
			// keep conn
			newConns = append(newConns, conn)
		} else {
			log.Println("Removing timed out connection")
		}
	}
	sort.Sort(byTimeout(newConns))
	p.conns = newConns

	// Add connections to get pool up to the MinSize
	connsNeeded := p.MinSize - len(p.conns)
	if connsNeeded > 0 {
		log.Printf("Adding %d connections to pool", connsNeeded)
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
	c, err := p.Dial()
	if err != nil {
		return nil, err
	} else {
		conn := &pooledConn{p, idletiming.Conn(c, p.IdleTimeout, nil)}
		return conn, nil
	}
}

type pooledConn struct {
	pool *Pool
	conn *idletiming.IdleTimingConn
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

type byTimeout []*pooledConn

func (a byTimeout) Len() int           { return len(a) }
func (a byTimeout) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byTimeout) Less(i, j int) bool { return a[i].conn.TimesOutAt().Before(a[j].conn.TimesOutAt()) }
