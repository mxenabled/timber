package main

//
// import (
// 	"bufio"
// 	"encoding/json"
// 	"fmt"
// 	"os"
// 	"regexp"
// 	"strings"
// 	"time"
//
// 	"github.com/kr/pretty"
// )
//
// // 5 MB
// const maxBufferLength = 5242880
//
// var (
// 	RegexBeginningOfLine = regexp.MustCompile(`^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.*LOG:`)
// 	RegexEndOfQuery      = regexp.MustCompile(`;$`)
// )
//
// // Objective 1: Be able to parse a slow query from the logs.
// //		- In order to do this we can look for the LOG starting and
// //      read until the terminating ';\r\n' of the query.
//
// // 	{
// // 	 "Query Text": "SELECT count(*) from shards group by \nguid,\nid,\ncreated_at;",
// // 	 "Plan": {
// // 	   "Node Type": "Aggregate",
// // 	   "Strategy": "Hashed",
// // 	   "Partial Mode": "Simple",
// // 	   "Parallel Aware": false,
// // 	   "Startup Cost": 378.50,
// // 	   "Total Cost": 383.50,
// // 	   "Plan Rows": 500,
// // 	   "Plan Width": 57,
// // 	   "Group Key": ["id"],
// // 	   "Plans": [
// // 	     {
// // 	       "Node Type": "Seq Scan",
// // 	       "Parent Relationship": "Outer",
// // 	       "Parallel Aware": false,
// // 	       "Relation Name": "shards",
// // 	       "Alias": "shards",
// // 	       "Startup Cost": 0.00,
// // 	       "Total Cost": 376.00,
// // 	       "Plan Rows": 500,
// // 	       "Plan Width": 49
// // 	     }
// // 	   ]
// // 	 }
// // 	}
// type AutoExplainedQueryPlan struct {
// 	QueryText string                 `json:"Query Text"`
// 	Plan      map[string]interface{} `json:"Plan"`
// }
//
// type SlowQuery struct {
// 	User      string
// 	Database  string
// 	Statement string
// 	Timestamp time.Time
// 	Duration  time.Duration
// }
//
// type LogLine struct {
// 	alive bool
// 	line  string
// }
//
// func parseSlowQuery(buffer string) (*SlowQuery, error) {
// 	var index int
//
// 	sq := new(SlowQuery)
//
// 	// Parse the statement
// 	index = strings.Index(buffer, "statement: ")
// 	if index < 0 {
// 		return nil, nil
// 	}
// 	sq.Statement = strings.Replace(buffer[index:], "statement: ", "", 1)
//
// 	// Parse the duration
// 	index = strings.Index(buffer, "duration: ")
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
// 		return nil, err
// 	}
// 	sq.Duration = duration
//
// 	// Parse User and Database
// 	index = strings.Index(buffer, " LOG:")
// 	if index < 0 {
// 		return nil, nil
// 	}
// 	splitsUpToUserAndDatabase := strings.Split(buffer[:index], " ")
// 	userAndDatabaseStr := splitsUpToUserAndDatabase[len(splitsUpToUserAndDatabase)-1]
// 	userAndDatabase := strings.Split(userAndDatabaseStr, "@")
// 	sq.User = userAndDatabase[0]
// 	sq.Database = userAndDatabase[1]
//
// 	// Parse Time
// 	timeStr := strings.Split(buffer, " [")[0]
// 	sq.Timestamp, err = time.Parse("2006-01-02 15:04:05 MST", timeStr)
// 	return sq, err
// }
//
// func processLogBuffer(buffer string) {
// 	if !isQueryPlan(buffer) {
// 		return
// 	}
//
// 	buffer = strings.Split(buffer, " plan:")[1]
//
// 	queryPlan := new(AutoExplainedQueryPlan)
//
// 	buf := []byte(buffer)
// 	err := json.Unmarshal(buf, &queryPlan)
// 	if err != nil {
// 		fmt.Println("Error parsing query plan from log:", err)
// 		return
// 	}
//
// 	pretty.Println("---")
// 	pretty.Println("SLOW QUERY:", queryPlan)
// }
//
// func isQueryPlan(buffer string) bool {
// 	return strings.Contains(buffer, " LOG:  ") && strings.Contains(buffer, " plan:")
// }
//
// func isSlowQuery(buffer string) bool {
// 	return strings.Contains(buffer, "  LOG: duration: ")
// }
//
// func isNewLogLine(line string) bool {
// 	return RegexBeginningOfLine.MatchString(line)
// }
//
// func isEndOfQuery(line string) bool {
// 	return RegexEndOfQuery.MatchString(line)
// }
//
// func mainiiwfoiwjfweif() {
// 	logLineChan := make(chan LogLine)
// 	logTimeout := time.NewTimer(time.Second)
//
// 	go func() {
// 		scanner := bufio.NewScanner(os.Stdin)
// 		for scanner.Scan() {
// 			logLineChan <- LogLine{line: scanner.Text(), alive: true}
// 		}
// 		logLineChan <- LogLine{line: "", alive: false}
// 	}()
//
// 	// I know, this is dumb.
// 	var buffer string
// 	for {
// 		logTimeout.Reset(time.Second)
//
// 		select {
// 		case logLine := <-logLineChan:
// 			if !logLine.alive {
// 				if len(buffer) > 0 {
// 					processLogBuffer(buffer)
// 				}
// 				return
// 			}
// 			line := logLine.line
//
// 			// New line
// 			if isNewLogLine(line) {
// 				if len(buffer) > 0 {
// 					processLogBuffer(buffer)
// 				}
// 				buffer = line
// 			} else {
// 				if len(buffer) < maxBufferLength {
// 					// These are stripped by the scanner.
// 					buffer += "\r\n"
// 					buffer += line
// 				} else {
// 					continue
// 				}
//
// 				//fmt.Println(line, []byte(line), isEndOfQuery(line))
// 				if isEndOfQuery(line) {
// 					processLogBuffer(buffer)
// 					buffer = ""
// 				}
// 			}
// 		case <-logTimeout.C:
// 			// The log timed out. Attempt to parse the entire line.
// 			// Skip if buffer is empty
// 			if len(buffer) == 0 {
// 				continue
// 			}
//
// 			//line := buffer
// 			//fmt.Println(line, []byte(line), isEndOfQuery(line))
// 			processLogBuffer(buffer)
// 			buffer = ""
// 		}
// 	}
//
// 	fmt.Println("Done")
// }
//
// func main2() {
// 	scanner := bufio.NewScanner(os.Stdin)
// 	// I know, this is dumb.
// 	var buffer string
// 	for scanner.Scan() {
// 		line := scanner.Text()
// 		if isNewLogLine(line) {
// 			if len(buffer) > 0 {
// 				processLogBuffer(buffer)
// 			}
// 			buffer = line
// 		} else {
// 			if len(buffer) < maxBufferLength {
// 				// These are stripped by the scanner.
// 				buffer += "\r\n"
// 				buffer += line
// 			} else {
// 				continue
// 			}
//
// 			fmt.Println(line, []byte(line), isEndOfQuery(line))
// 			if isEndOfQuery(line) {
// 				processLogBuffer(buffer)
// 				buffer = ""
// 			}
// 		}
// 	}
// 	fmt.Println("Done")
// }
