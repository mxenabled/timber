package main

import (
	"bytes"
	"fmt"
	"io"
	"log/syslog"
)

var (
	KibanaLogger io.Writer
)

func init() {
	var err error
	KibanaLogger, err = syslog.New(syslog.LOG_LOCAL1, "postgres_slow_query_logger")
	if err != nil {
		panic(err)
	}
}

func SendToKibana(b []byte) {
	buffer := bytes.NewBuffer(b)
	_, err := io.Copy(KibanaLogger, buffer)
	if err != nil {
		fmt.Println("Failed to write message to kibana:", err)
	}
}
