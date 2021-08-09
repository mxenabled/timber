package main

import (
	"io/ioutil"
	"log"
	"net"
	"testing"
	"time"
)

func TestTCPLoggerBasic(t *testing.T) {
	server, client := net.Pipe()

	tcpLogger := NewTCPLogger(client, 10)
	tcpLogger.Start()
	tcpLogger.Write([]byte(`{"test": "testing"}\r\n`))

	// TCPLogger goroutine needs a chance to process the message before test makes assertions.
	time.Sleep(time.Millisecond * 10)
	log.Println(len(tcpLogger.logLines))

	if len(tcpLogger.logLines) > 0 {
		t.Fatal("TCPLogger.logLines still had messages!")
	}

	tcpLogger.Close()
	server.Close()
}

func TestTCPLoggerRetry(t *testing.T) {
	// Server needs to accept a connnection for TCPLogger conn and then shut off
	// to simulate bad connection.
	server, client := net.Pipe()
	server.Close()

	tcpLogger := NewTCPLogger(client, 10)
	// Slower sleepDuration at retryLimit in tests to ensure that the go routine
	// that processes messages wakes up and retries the message to assert
	// the channel successfully drains.
	tcpLogger.sleepDuration = time.Millisecond * 10
	tcpLogger.Start()

	msg := []byte(`{"test": "testing"}\r\n`)
	tcpLogger.Write(msg)

	// TCPLogger needs a chance for the goroutine to increment retries.
	time.Sleep(time.Millisecond * 50)
	if len(tcpLogger.logLines) < 0 {
		t.Fatal("After server close, TCPLogger.logLines should not be empty if there was a failed write.")
	}
	if tcpLogger.retries < 1 {
		t.Fatal("After server close, and message attempted to be written, TCPLogger.retries should >= 1")
	}

	// Set up new server/client connection and swap the connection in TCPLogger
	server, client = net.Pipe()
	tcpLogger.swapConn(client)

	time.Sleep(time.Millisecond * 50)
	go func() {
		time.Sleep(time.Millisecond * 25)
		client.Close()
	}()

	b, err := ioutil.ReadAll(server)
	if err != nil {
		t.Fatal(err)
	}

	stripCloseBytes := b[:len(b)-2] // Closing the writer also sends CR & LF/NL bytes
	if string(stripCloseBytes) != string(msg) {
		t.Fatalf("Unexpected message written to pipe, got:%s, wanted:%s", stripCloseBytes, msg)
	}

	if len(tcpLogger.logLines) > 0 {
		t.Fatal("After client connection swap to live connection, TCPLogger.logLines should successfully push!")
	}

	tcpLogger.Close()
	server.Close()
}
