package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"
)

var (
	RegexBeginningOfLine = regexp.MustCompile(`^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.*([A-Z]+):`)
)

func HandlePostgresLogLine(logLine *PostgresLogLine) {
	switch logLine.LogType {
	case "statement", "execute", "parse", "bind":
		LogSlowQuery(logLine)
	case "plan":
		// NOTHING FOR NOW
	}
}

type LogScanner interface {
	Scan() bool
	Text() string
	Err() error
}

func NewStdinLogScanner() LogScanner {
	return bufio.NewScanner(os.Stdin)
}

type LogLine struct {
	alive bool
	line  string
}

type PostgresLogLine struct {
	Timestamp     time.Time
	Username      string
	Database      string
	Duration      time.Duration
	LogType       string
	StatementName string
	Value         string
	ShardPartition string
	PartitionlessQuery string
}

type PostgresLogParser struct {
	logScanner  LogScanner
	buffer      string
	logLineChan chan *LogLine
}

func NewPostgresLogParser(logScanner LogScanner) *PostgresLogParser {
	logLineChan := make(chan *LogLine)

	// Continue to parse the scanner for log lines.
	// Signal when the scanner has completed.
	go func() {
		for logScanner.Scan() {
			logLineChan <- &LogLine{line: logScanner.Text(), alive: true}
		}
		close(logLineChan)
	}()

	return &PostgresLogParser{
		buffer:      "",
		logLineChan: logLineChan,
		logScanner:  logScanner,
	}
}

// 5 MB
const maxBufferLength = 5242880

var (
	ErrLogEOF         = errors.New("EOF: The log has ended")
	ErrInvalidLogLine = errors.New("The parser could not derive query or plan info from the log line")
)

// A postgres log line starts with a "YYYY-MM-DD HH:MM:SS [*] LOG:" pattern.
// If a line matches that, it's a new postgres log line.
// We can continue adding random unmatched newlines to the buffer after detecting
// a new log line, because postgres log lines can have multiple lines.
// Lastly, postgres doesn't hesitate when it logs lines, so we can also include a
// timer to detect the end of a postgres log line.
func (self *PostgresLogParser) Parse() (*PostgresLogLine, error) {
	logTimeout := time.NewTimer(time.Second)

	for {
		// Reset the log line timeout timer.
		logTimeout.Reset(time.Second)

		// Collect a single log line.
		select {
		case logLine := <-self.logLineChan:
			// Handle EOF from the log scanner.
			if logLine == nil || !logLine.alive {
				if len(self.buffer) > 0 {
					// Parse the line and then return
					return self.parseLogBuffer()
				} else {
					// Return EOF
					return nil, ErrLogEOF
				}
			}

			// If we detect a new log line and we have existing buffer,
			// then we need to parse the buffer. And reset buffer.
			rawLine := logLine.line
			if len(self.buffer) > 0 && isNewLogLine(rawLine) {
				// Swap buffer so rawLine can be included next time Parse is called.

				// Time to parse this and return to caller.
				log, err := self.parseLogBuffer()
				self.buffer = rawLine
				return log, err
			}

			// Otherwise, we can continue adding buffer until max buffer size.
			if len(self.buffer) < maxBufferLength {
				self.buffer += "\r\n"
				self.buffer += rawLine
			}

		case <-logTimeout.C:
			if len(self.buffer) == 0 {
				continue
			}

			return self.parseLogBuffer()
		}
	}
}

func (self *PostgresLogParser) parseLogBuffer() (*PostgresLogLine, error) {
	// Parse Duration
	index := strings.Index(self.buffer, "duration: ")
	if index < 0 {
		self.buffer = ""
		return nil, ErrInvalidLogLine
	}
	durationEtc := self.buffer[index:]
	durationEndIndex := strings.Index(durationEtc, " ms")
	if durationEndIndex < 0 {
		self.buffer = ""
		return nil, ErrInvalidLogLine
	}
	durationEndIndex += index
	duration, _ := time.ParseDuration(
		fmt.Sprint(
			strings.Replace(self.buffer[index:durationEndIndex], "duration: ", "", 1),
			"ms"))

	timestamp := parseTime(self.buffer)
	user, database := parseUserAndDatabase(self.buffer)
	logType, statementName := parseLogTypeWithStatementName(self.buffer)
	value, shardPartition, partitionlessQuery := parseValuesFromBuffer(self.buffer)

	log := &PostgresLogLine{
		Timestamp:     timestamp,
		Username:      user,
		Database:      database,
		Duration:      duration,
		LogType:       logType,
		StatementName: statementName,
		Value:         value,
		ShardPartition: shardPartition,
		PartitionlessQuery: partitionlessQuery,
	}

	self.buffer = ""
	return log, nil
}

func parseShardFromValue(value string) string {
	shardPartition := ""
	if strings.Contains(value, "abacus") {
		splitStart := strings.Split(value, "FROM ")[1]
		splitEnd := strings.Split(splitStart, " ")[0]
		shardPartition = strings.Split(splitEnd, ".")[0]
	}
	return shardPartition
}

// Parse Time
func parseTime(buffer string) time.Time {
	timeStr := strings.Split(buffer, " [")[0]
	timestamp, _ := time.Parse("2006-01-02 15:04:05 MST", timeStr)
	return timestamp
}

// Parse Log Type w/ StatementName
func parseLogTypeWithStatementName(buffer string) (string, string) {
	partial := strings.Split(buffer, " ms  ")[1]
	partial = strings.Split(partial, ":")[0]
	result := strings.Split(partial, " ")
	if len(result) > 1 {
		return result[0], result[1]
	}
	return result[0], ""
}

// Parse value from buffer
func parseValuesFromBuffer(buffer string) (string, string, string) {
	partial := strings.Split(buffer, " ms  ")[1]
	index := strings.Index(partial, ": ")
	value := strings.SplitN(partial, ":", 2)[1]
	if index > 0 {
		value = strings.SplitN(partial, ": ", 2)[1]
	}

	shardPartition := parseShardFromValue(value)
	partitionlessQuery := ""

	// Remove the shardPartition from value so it can be aggregatable
	if strings.Contains(shardPartition, "abacus") {
		 partitionlessQuery = strings.Replace(value, (shardPartition + "."), "", -1)
	}
	return value, shardPartition, partitionlessQuery
}

// Parse User and Database
func parseUserAndDatabase(buffer string) (string, string) {
	index := strings.Index(buffer, " LOG:")
	if index < 0 {
		return "", ""
	}
	splitsUpToUserAndDatabase := strings.Split(buffer[:index], " ")
	userAndDatabaseStr := splitsUpToUserAndDatabase[len(splitsUpToUserAndDatabase)-1]
	userAndDatabase := strings.Split(userAndDatabaseStr, "@")
	user := userAndDatabase[0]
	database := userAndDatabase[1]
	return user, database
}

func isNewLogLine(line string) bool {
	return RegexBeginningOfLine.MatchString(line)
}

var (
	loggerSourceType string
	displayVersion   bool

	version string = "0.0.2"
)

func main() {
	flag.StringVar(&loggerSourceType, "logger-source-type", "stdin", "supports stdin for piped input and journald")
	flag.BoolVar(&displayVersion, "version", false, "show the version and exit")
	flag.Parse()

	if displayVersion {
		fmt.Println(version)
		return
	}

	var logScanner LogScanner
	var err error

	switch loggerSourceType {
	case "journald":
		logScanner, err = NewJournaldLogScanner()
		if err != nil {
			fmt.Println("Could not start the journald logger source:", err)
			return
		}
	case "stdin":
		logScanner = NewStdinLogScanner()
	default:
		fmt.Println("Uknown logger source type:", loggerSourceType)
		return
	}

	logParser := NewPostgresLogParser(logScanner)
	for {
		pgLogLine, err := logParser.Parse()
		if err == ErrLogEOF {
			return
		}
		if err == ErrInvalidLogLine {
			fmt.Println("Skipping log line:", err)
			continue
		}
		if err != nil {
			fmt.Println("Error parsing postgres log:", err)
			continue
		}

		HandlePostgresLogLine(pgLogLine)
	}
}
