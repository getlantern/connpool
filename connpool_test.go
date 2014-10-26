package connpool

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/getlantern/testify/assert"
	"github.com/getlantern/waitforserver"
)

var (
	msg = []byte("HELLO")
)

func TestIt(t *testing.T) {
	poolSize := 20
	claimTimeout := 1 * time.Second
	fillTime := 100 * time.Millisecond

	addr, err := startTestServer()
	if err != nil {
		t.Fatalf("Unable to start test server: %s", err)
	}
	p := &Pool{
		MinSize:      poolSize,
		ClaimTimeout: claimTimeout,
		Dial: func() (net.Conn, error) {
			return net.DialTimeout("tcp", addr, 15*time.Millisecond)
		},
	}

	fdCountStart := countOpenFiles()

	p.Start()
	time.Sleep(fillTime)

	openConns := countOpenFiles() - fdCountStart
	assert.Equal(t, poolSize, openConns, "Pool should initially open the right number of conns")

	// Use more than the pooled connections
	connectAndRead(t, p, poolSize*2)

	time.Sleep(fillTime)
	openConns = countOpenFiles() - fdCountStart
	assert.Equal(t, poolSize, openConns, "Pool should fill itself back up to the right number of conns")

	// Wait for connections to time out
	time.Sleep(claimTimeout * 2)

	// Test our connections again
	connectAndRead(t, p, poolSize*2)

	time.Sleep(fillTime)
	openConns = countOpenFiles() - fdCountStart
	assert.Equal(t, poolSize, openConns, "After pooled conns time out, pool should fill itself back up to the right number of conns")

	p.Stop()
	time.Sleep(2 * time.Second)

	openConns = countOpenFiles() - fdCountStart
	assert.Equal(t, 0, openConns, "After stopping pool and allowing it to quiesce, there should be no more open conns")
}

func connectAndRead(t *testing.T, p *Pool, loops int) {
	for i := 0; i < loops; i++ {
		c, err := p.Get()
		if err != nil {
			t.Fatalf("Error getting connection: %s", err)
		}
		read, err := ioutil.ReadAll(c)
		if err != nil {
			t.Fatalf("Error reading from connection: %s", err)
		}
		assert.Equal(t, msg, read, "Should have received %s from server", string(msg))
		c.Close()
	}
}

func startTestServer() (string, error) {
	l, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		return "", err
	}
	err = waitforserver.WaitForServer("tcp", l.Addr().String(), 1*time.Second)
	if err != nil {
		return "", err
	}
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				log.Fatalf("Error listening: %s", err)
			}
			_, err = c.Write(msg)
			if err != nil {
				log.Fatalf("Unable to write message: %s", err)
			}
			c.Close()
		}
	}()
	return l.Addr().String(), nil
}

// see https://groups.google.com/forum/#!topic/golang-nuts/c0AnWXjzNIA
func countOpenFiles() int {
	out, err := exec.Command("lsof", "-p", fmt.Sprintf("%v", os.Getpid())).Output()
	if err != nil {
		log.Fatal(err)
	}
	return bytes.Count(out, []byte("\n")) - 1
}
