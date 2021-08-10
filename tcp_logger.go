package main

import (
	"io"
	"log"
	"net"
	"os"
	"time"
)

// TCPLogger is a logging service that will spool messages in a local channel
// that are intended to be flushed out to a net.Conn given.
// Note that messages are not guaranteed to be delivered, but attempts are made to
// redeliver messages given connection.Write errors.
type TCPLogger struct {
	conn          net.Conn
	deadlineWait  time.Duration
	retryLimit    int
	retries       int
	sleepDuration time.Duration

	logLines chan []byte
}

// NewTCPLogger is used to establish a new TCPLogger.
func NewTCPLogger(conn net.Conn, retryLimit int) TCPLogger {
	return TCPLogger{
		conn:          conn,
		deadlineWait:  time.Minute,
		retryLimit:    retryLimit,
		sleepDuration: time.Minute,

		logLines: make(chan []byte, 1000),
	}
}

// Write pushes bytes given into local chan to flush out to connection.
func (t *TCPLogger) Write(p []byte) (n int, err error) {
	select {
	case t.logLines <- p:
		return len(p), nil
	default:
		log.Println("Error: Could not log to TCP output because the queue is full")
		return 0, nil // doesn't look like we care about the response
	}
}

// Start call this to start the go routine that will output
// from the logLines and output it into the established net.Conn
func (t *TCPLogger) Start() {
	go func() {
		for {
			logLine := <-t.logLines

			t.conn.SetDeadline(time.Now().Add(t.deadlineWait))
			_, err := t.conn.Write(logstashDelimit(logLine))
			if err != nil {
				t.handleConnError(err, logLine)
			} else {
				t.retries = 0
			}
		}
	}()
}

func (t *TCPLogger) handleConnError(err error, b []byte) {
	log.Println("Error while writing to TCPLogger.conn:", err.Error())
	if err == os.ErrDeadlineExceeded || err == io.EOF {
		t.RetryConnection()
	}

	if t.retries <= t.retryLimit {
		t.logLines <- b
		t.retries += 1
	} else {
		log.Println("Retry limit met on TCPLogger.. sleeping for a minute before continuing.")
		t.logLines <- b
		time.Sleep(t.sleepDuration)
		t.RetryConnection()
		t.retries = 0
	}
}

// RetryConnection will attempt to reestablish connection to net.Conn
func (t *TCPLogger) RetryConnection() error {
	log.Println("Attempting tcp connection reestablish...")
	conn, err := net.Dial(t.conn.RemoteAddr().Network(), t.conn.RemoteAddr().String())
	if err != nil {
		log.Println("Error reconnecting:", err)
		return err
	}
	t.swapConn(conn)
	return nil
}

func (t *TCPLogger) swapConn(conn net.Conn) {
	t.conn.Close()
	t.conn = conn
}

// Close defer this to ensure any existing messages in the channel that have not yet made it
// to connection destination get pushed before closing connection.
func (t TCPLogger) Close() {
	ticker := time.NewTicker(time.Second * 5)

	for len(t.logLines) > 0 {
		select {
		case <-ticker.C:
			log.Println("Error: Time limit exceeded for graceful shutdown of TCPLogger!")
			t.conn.Close()
			return
		default:
			if t.retries > t.retryLimit {
				log.Printf("Retry limit met in close function.. %d message(s) will be lost!\n", len(t.logLines))
				break
			}
			time.Sleep(time.Millisecond * 50)
		}
	}
	t.conn.Close()
}

// logstash requires carriage return delimiter between
// message writes.
func logstashDelimit(b []byte) []byte {
	return append(b, []byte("\r\n")...)
}
