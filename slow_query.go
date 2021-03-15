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
	RegexHasShard  = regexp.MustCompile(`(?i)from\s+(\w+|"\w+")\.`)
)

func ScrubQuery(sql string) string {
	sql = RegexSqlString.ReplaceAllString(sql, "'XXX'")
	sql = RegexNString.ReplaceAllString(sql, "N")
	// The above is very aggressive filtering
	// need to be more specific on what gets scrubbed

	// TODO: This is where to filter out the PII data
	return sql
}

func ParseShardFromValue(value string) string {
	// This assumes a query has only one schema.
	// Multi-schema queries will not be parsed correctly.
	shardName := ""
	possibleShard := RegexHasShard.FindStringSubmatch(value)
	if len(possibleShard) > 0 {
		shardName = possibleShard[1]
	}

	return shardName
}

func DerivedValues(value string) (string, string) {
	shardName := ParseShardFromValue(value)
	cleanedShardName := strings.Replace(shardName, "\"", "", -1)

	shardlessQuery := value

	// Remove the shardName from value so it can be aggregatable
	if shardName != "" {
		shardlessQuery = strings.Replace(value, (shardName + "."), "", -1)
	}
	return cleanedShardName, shardlessQuery
}

type SlowQueryMessage struct {
	Command                string  `json:"command"`
	Query                  string  `json:"query"`
	Database               string  `json:"database"`
	Username               string  `json:"username"`
	ShardName              string  `json:"shard_name"`
	ShardlessQuery         string  `json:"shardless_query"`
	DurationInMilliseconds float64 `json:"duration_in_milliseconds"`
	CreatedAt              string  `json:"created_at"`
	Type                   string  `json:"type"`
}

func LogSlowQuery(logLine *PostgresLogLine) {
	shardName, shardlessQuery := DerivedValues(logLine.Value)
	msg := &SlowQueryMessage{
		Command:                logLine.LogType,
		Query:                  ScrubQuery(logLine.Value),
		Database:               logLine.Database,
		Username:               logLine.Username,
		ShardName:              shardName,
		ShardlessQuery:         ScrubQuery(shardlessQuery),
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
