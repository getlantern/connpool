package connpool

import (
	"net"
	"time"

	"github.com/getlantern/golog"
)

const (
	DefaultClaimTimeout = 10 * time.Minute
	TimeoutThreshold    = 1 * time.Second
)

type DialFunc func() (net.Conn, error)

// Pool is a pool of connections.  Connections are pooled eagerly up to MinSize
// and expire after ClaimTimeout.  Pool attempts to always have MinSize
// unexpired connections ready to go so that callers don't have to wait on a
// connection being established when they're ready to use it.
type Pool struct {
	// MinSize: the pool will always attempt to maintain at least these many
	// connections.
	MinSize int

	// ClaimTimeout: connections will be removed from pool if unclaimed for
	// longer than ClaimTimeout.  The default ClaimTimeout is 10 minutes.
	ClaimTimeout time.Duration

	// Dial: specifies the function used to create new connections
	Dial DialFunc

	log    golog.Logger
	connCh chan *pooledConn
	stopCh chan interface{}
}

// Start starts the pool, filling it to the MinSize and maintaining fresh
// connections.
func (p *Pool) Start() {
	p.log = golog.LoggerFor("connpool")

	p.log.Trace("Starting connection pool")
	if p.ClaimTimeout == 0 {
		p.log.Tracef("Defaulting ClaimTimeout to %s", DefaultClaimTimeout)
		p.ClaimTimeout = DefaultClaimTimeout
	}

	p.connCh = make(chan *pooledConn)
	p.stopCh = make(chan interface{}, p.MinSize)

	for i := 0; i < p.MinSize; i++ {
		go p.feedConn()
	}
}

func (p *Pool) Stop() {
	p.log.Trace("Stopping all feedConn goroutines")
	for i := 0; i < p.MinSize; i++ {
		select {
		case p.stopCh <- nil:
			p.log.Trace("Stop requested")
		default:
			p.log.Trace("Stop previously requested")
		}
	}
}

func (p *Pool) Get() (net.Conn, error) {
	p.log.Trace("Getting conn")
	for {
		select {
		case pc := <-p.connCh:
			p.log.Trace("Looking for an unexpired pooled conn")
			if pc.expires.After(time.Now()) {
				p.log.Trace("Using pooled conn")
				return pc.conn, nil
			} else {
				p.log.Trace("Closing expired pooled conn")
				pc.conn.Close()
			}
		default:
			p.log.Trace("No pooled conn, dialing our own")
			pc, err := p.dial()
			if err != nil {
				return nil, err
			}
			return pc.conn, nil
		}
	}
}

// feedConn works on continuously feeding the connCh with fresh connections.
func (p *Pool) feedConn() {
	newConnTimedOut := time.NewTimer(0)

	for {
		p.log.Trace("Dialing")
		pc, err := p.dial()
		if err != nil {
			p.log.Tracef("Error dialing: %s", err)
			continue
		}
		newConnTimedOut.Reset(p.ClaimTimeout)

		select {
		case p.connCh <- pc:
			p.log.Trace("Fed conn")
		case <-newConnTimedOut.C:
			p.log.Trace("Queued conn timed out, closing")
			pc.conn.Close()
		case <-p.stopCh:
			// Close unqueued conn
			pc.conn.Close()
			// Close all queued conns
			for {
				select {
				case pc := <-p.connCh:
					pc.conn.Close()
				default:
					break
				}
			}
			p.log.Trace("Stopped feeding conn")
			return
		}
	}
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
