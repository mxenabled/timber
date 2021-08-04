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

		logLines: make(chan []byte, 1000), // Should this be a buffered channel?
	}
}

// Write pushes bytes given into local chan to flush out to connection.
func (t *TCPLogger) Write(p []byte) (n int, err error) {
	t.logLines <- p
	return len(p), nil
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
				log.Println("But this is resetting it..?")
				t.retries = 0
			}
		}
	}()
}

func (t *TCPLogger) handleConnError(err error, b []byte) {
	if err == os.ErrDeadlineExceeded || err == io.EOF {
		t.RetryConnection()
	}

	if t.retries <= t.retryLimit {
		t.retries += 1
		t.logLines <- b
	} else {
		log.Println("Retry limit met on TCPLogger.. sleeping for a minute before continuing.")
		t.logLines <- b
		time.Sleep(t.sleepDuration)
		t.retries = 0
	}
}

// RetryConnection will attempt to reestablish connection to net.Conn
func (t *TCPLogger) RetryConnection() error {
	conn, err := net.Dial(t.conn.RemoteAddr().Network(), t.conn.RemoteAddr().String())
	if err != nil {
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
	for len(t.logLines) > 0 {
		if t.retries > t.retryLimit {
			log.Printf("Retry limit met in close function.. %d message(s) will be lost!\n", len(t.logLines))
			break
		}
		time.Sleep(time.Millisecond * 50)
	}
	// t.done <- true
	t.conn.Close()
}

// logstash requires carriage return delimiter between
// message writes.
func logstashDelimit(b []byte) []byte {
	return append(b, []byte("\r\n")...)
}
