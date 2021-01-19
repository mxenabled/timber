package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os/exec"

	"github.com/kr/pretty"
)

type JournalMessage struct {
	Message   string `json:"MESSAGE"`
	Priority  string `json:"PRIORITY"`
	Facility  string `json:"SYSLOG_FACILITY"`
	Tag       string `json:"SYSLOG_IDENTIFIER"`
	BootId    string `json:"_BOOT_ID"`
	Exe       string `json:"_EXE"`
	Gid       string `json:"_GID"`
	HostName  string `json:"_HOSTNAME"`
	MachineId string `json:"_MACHINE_ID"`
	PID       string `json:"_PID"`
	Unit      string `json:"_SYSTEMD_UNIT"`
	Transport string `json:"_TRANSPORT"`
	UID       string `json:"_UID"`
	Timestamp string `json:"__REALTIME_TIMESTAMP"`
}

func journalctl(tag string) (*bufio.Scanner, error) {
	c := exec.Command("/usr/bin/journalctl", "-t", tag, "-f", "-o", "json")
	stdout, err := c.StdoutPipe()
	if err != nil {
		return nil, err
	}
	err = c.Start()
	if err != nil {
		return nil, err
	}
	return bufio.NewScanner(stdout), nil
}

func mainYolo() {
	scanner, err := journalctl("yolo")
	if err != nil {
		panic(err)
	}
	msg := new(JournalMessage)
	for scanner.Scan() {
		bytes := []byte(scanner.Text())
		err := json.Unmarshal(bytes, msg)
		if err != nil {
			fmt.Println("Error parsing journal line:", err)
			continue
		}
		pretty.Println(msg.Message)
	}
}
