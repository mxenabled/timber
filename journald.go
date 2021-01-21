package main

import (
	"bufio"
	"encoding/json"
	"os/exec"
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

type JournaldScanner struct {
	scanner     *bufio.Scanner
	nextMessage *JournalMessage
	nextError   error
}

func NewJournaldLogScanner() (LogScanner, error) {
	scanner, err := journalctl("postgres")
	if err != nil {
		return nil, err
	}
	return &JournaldScanner{
		scanner:     scanner,
		nextMessage: new(JournalMessage),
	}, nil
}

func (self *JournaldScanner) Scan() bool {
	self.nextError = nil

	if !self.scanner.Scan() {
		self.nextError = self.scanner.Err() // ???
		return false
	}

	bytes := []byte(self.scanner.Text())
	err := json.Unmarshal(bytes, self.nextMessage)
	if err != nil {
		self.nextError = err
		return false
	}

	return true
}

func (self *JournaldScanner) Text() string {
	return self.nextMessage.Message
}

func (self *JournaldScanner) Err() error {
	return self.nextError
}
