package main

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/kr/pretty"
)

var (
	RegexBeginningOfLine = regexp.MustCompile(`^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.*LOG:`)
)

func HandlePostgresLogLine(logLine *PostgresLogLine) {
	switch logLine.LogType {
	case "statement", "execute", "parse", "bind":
		// LogSlowQuery(logLine)
	case "plan":
		// LogQueryPlan(logLine)
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

var ErrLogEOF = errors.New("EOF: The log has ended")

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
		return nil, nil
	}
	durationEtc := self.buffer[index:]
	durationEndIndex := strings.Index(durationEtc, " ms")
	if durationEndIndex < 0 {
		return nil, nil
	}
	durationEndIndex += index
	duration, _ := time.ParseDuration(
		fmt.Sprint(
			strings.Replace(self.buffer[index:durationEndIndex], "duration: ", "", 1),
			"ms"))

	timestamp := parseTime(self.buffer)
	logType := parseLogType(self.buffer)
	user, database := parseUserAndDatabase(self.buffer)
	statmentName, _ := parseStatementName(self.buffer)
	// duration := parseDuration(self.buffer)

	log := &PostgresLogLine{
		Timestamp:     timestamp,
		Username:      user,
		Database:      database,
		Duration:      duration,
		LogType:       logType,
		StatementName: statmentName,
		Value:         self.buffer,
	}

	self.buffer = ""
	return log, nil
}

// Parse Time
func parseTime(buffer string) time.Time {
	timeStr := strings.Split(buffer, " [")[0]
	timestamp, _ := time.Parse("2006-01-02 15:04:05 MST", timeStr)
	return timestamp
}

// Parse Log Type
func parseLogType(buffer string) string {
	partial := strings.Split(buffer, " ms  ")[1]
	return strings.Split(partial, ":")[0]
}

// Parse the statementName
func parseStatementName(buffer string) (string, error) {
	index := strings.Index(buffer, "statement: ")
	if index < 0 {
		return "", nil
	}
	return strings.Replace(buffer[index:], "statement: ", "", 1), nil
}

// // Parse the duration
// func parseDuration(buffer string) (*time.Duration, error) {
// 	index := strings.Index(buffer, "duration: ")
// 	if index < 0 {
// 		return nil, nil
// 	}
// 	durationEtc := buffer[index:]
// 	durationEndIndex := strings.Index(durationEtc, " ms")
// 	if durationEndIndex < 0 {
// 		return nil, nil
// 	}
// 	durationEndIndex += index
// 	duration, err := time.ParseDuration(
// 		fmt.Sprint(
// 			strings.Replace(buffer[index:durationEndIndex], "duration: ", "", 1),
// 			"ms"))
// 	if err != nil {
// 		return nil, nil
// 	}
// 	return duration
// }

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

func main() {
	// 	logStreamer, err := NewJournaldLogStreamer("postgresql")

	logScanner := NewStdinLogScanner()
	logParser := NewPostgresLogParser(logScanner)

	for {
		pgLogLine, err := logParser.Parse()
		if err == ErrLogEOF {
			return
		}
		if err != nil {
			fmt.Println("Error parsing postgres log:", err)
			continue
		}

		pretty.Println(pgLogLine)
	}
}
