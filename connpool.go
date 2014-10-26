package connpool

import (
	"net"
	"sync"
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
	connCh chan net.Conn
	stopCh chan *sync.WaitGroup
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

	p.connCh = make(chan net.Conn)
	p.stopCh = make(chan *sync.WaitGroup, p.MinSize)

	for i := 0; i < p.MinSize; i++ {
		go p.feedConn()
	}
}

// Stop stops the goroutines that are filling the pool, blocking until they've
// all temrinated.
func (p *Pool) Stop() {
	p.log.Trace("Stopping all feedConn goroutines")
	var wg sync.WaitGroup
	wg.Add(p.MinSize)
	for i := 0; i < p.MinSize; i++ {
		select {
		case p.stopCh <- &wg:
			p.log.Trace("Stop requested")
		default:
			p.log.Trace("Stop previously requested")
		}
	}
	wg.Wait()
}

func (p *Pool) Get() (net.Conn, error) {
	p.log.Trace("Getting conn")
	for {
		select {
		case conn := <-p.connCh:
			p.log.Trace("Using pooled conn")
			return conn, nil
		default:
			p.log.Trace("No pooled conn, dialing our own")
			return p.Dial()
		}
	}
}

// feedConn works on continuously feeding the connCh with fresh connections.
func (p *Pool) feedConn() {
	newConnTimedOut := time.NewTimer(0)

	for {
		p.log.Trace("Dialing")
		conn, err := p.Dial()
		if err != nil {
			p.log.Tracef("Error dialing: %s", err)
			continue
		}
		newConnTimedOut.Reset(p.ClaimTimeout)

		select {
		case p.connCh <- conn:
			p.log.Trace("Fed conn")
		case <-newConnTimedOut.C:
			p.log.Trace("Queued conn timed out, closing")
			conn.Close()
		case wg := <-p.stopCh:
			p.log.Trace("Stopping")
			p.log.Trace("Closing queued conn")
			conn.Close()
			p.log.Trace("Closing all queued conns")
		CloseLoop:
			for {
				select {
				case conn := <-p.connCh:
					p.log.Trace("Closing conn from pool")
					conn.Close()
				default:
					p.log.Trace("No more queued conns to close")
					break CloseLoop
				}
			}
			p.log.Trace("Stopped feeding conn")
			(*wg).Done()
			return
		}
	}
}
