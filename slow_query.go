package main

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
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

func parseShardFromValue(value string) string {
	shardPartition := ""
	splitStart := strings.Split(value, " FROM ")
	if len(splitStart) > 1 {
		splitEnd := strings.Split(splitStart[1], " ")[0]
		shardPartition = strings.Split(splitEnd, ".")[0]
	}

	return shardPartition
}

func derivedValues(value string) (string, string) {
	shardPartition := parseShardFromValue(value)
	cleanedShardPartition := strings.Replace(shardPartition, "\"", "", -1)

	partitionlessQuery := ""

	// Remove the shardPartition from value so it can be aggregatable
	if shardPartition != "" {
		partitionlessQuery = strings.Replace(value, (shardPartition + "."), "", -1)
	}
	return cleanedShardPartition, partitionlessQuery
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
	shardPartition, partitionlessQuery := derivedValues(logLine.Value)
	msg := &SlowQueryMessage{
		Command:                logLine.LogType,
		Query:                  ScrubQuery(logLine.Value),
		Database:               logLine.Database,
		Username:               logLine.Username,
		ShardPartition:         shardPartition,
		PartitionlessQuery:     ScrubQuery(partitionlessQuery),
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
