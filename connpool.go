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

// Start starts the pool, filling it to the MinSize and maintaining the
// connections.
func (p *Pool) Start() {
	p.log = golog.LoggerFor("connpool")

	p.log.Trace("Starting connection pool")
	if p.ClaimTimeout == 0 {
		p.log.Tracef("Defaulting ClaimTimeout to %s", DefaultClaimTimeout)
		p.ClaimTimeout = DefaultClaimTimeout
	}

	// Size the channel of conns to be 1 less than the min pool size, since
	// there will always be 1 conn that's dialed and waiting to be enqueued.
	channelSize := p.MinSize - 1
	if channelSize < 0 {
		channelSize = 0
	}
	p.connCh = make(chan *pooledConn, channelSize)
	p.stopCh = make(chan interface{})

	go p.process()
}

func (p *Pool) Stop() {
	select {
	case p.stopCh <- nil:
		p.log.Trace("Stop requested")
	default:
		p.log.Trace("Stop previously requested")
	}
}

func (p *Pool) Get() (net.Conn, error) {
	p.log.Trace("Getting conn from pool")
	for {
		select {
		case pc := <-p.connCh:
			p.log.Trace("Looking for an unexpired pooled conn")
			if pc.expires.After(time.Now()) {
				p.log.Trace("Using pooled conn")
				return pc.conn, nil
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

// process is the run loop for the Pool
func (p *Pool) process() {
	for {
		p.log.Trace("Dialing")
		pc, err := p.dial()
		if err != nil {
			p.log.Tracef("Error dialing: %s", err)
			continue
		}
		select {
		case p.connCh <- pc:
			p.log.Trace("Queued conn")
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
			p.log.Trace("Stopped")
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
