package main

import (
	"encoding/json"
	"fmt"
	"regexp"
	"time"

	"github.com/kr/pretty"
)

var (
	RegexSqlString = regexp.MustCompile("'[^']+'")
	RegexNString   = regexp.MustCompile(`([0-9])+`)
)

func ScrubQuery(sql string) string {
	sql = RegexSqlString.ReplaceAllString(sql, "'XXX'")
	sql = RegexNString.ReplaceAllString(sql, "N")

	return sql
}

type SlowQueryMessage struct {
	Command                string  `json:"command"`
	Query                  string  `json:"query"`
	Database               string  `json:"database"`
	Username               string  `json:"username"`
	ShardPartition         string  `json:"shard_partition"`
	PartitionlessQuery     string  `json:"partitionless_query"`
	DurationInMilliseconds float64 `json:"duration_in_milliseconds"`
	CreatedAt              string  `json:"created_at"`
	Type                   string  `json:"type"`
}

func LogSlowQuery(logLine *PostgresLogLine) {
	msg := &SlowQueryMessage{
		Command:                logLine.LogType,
		Query:                  ScrubQuery(logLine.Value),
		Database:               logLine.Database,
		Username:               logLine.Username,
		ShardPartition: 				logLine.ShardPartition,
		PartitionlessQuery: 		ScrubQuery(logLine.PartitionlessQuery),
		DurationInMilliseconds: float64(logLine.Duration.Microseconds()) / 1000.0,
		CreatedAt:              time.Now().UTC().String(),
		Type:                   "timber.postgres_slow_query",
	}

	bytes, err := json.Marshal(msg)
	if err != nil {
		fmt.Println("Could not encoding the slow query log as json:", err)
		return
	}

	SendToKibana(bytes)

	pretty.Println(string(bytes))
}
