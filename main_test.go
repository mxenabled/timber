package main

import (
	"bufio"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestParsingBindStatement(t *testing.T) {
	log := `2021-01-11 15:25:36 EST [56193-3/9939-5706] postgres@walle_test LOG:  duration: 0.139 ms  bind <unnamed>: SELECT 1 AS one FROM "borrower_applications" WHERE "borrower_applications"."confirmation_number" = $1 LIMIT $2
2021-01-11 15:25:36 EST [56193-3/9939-5707] postgres@walle_test DETAIL:  parameters: $1 = '6BE8-BC52-7545', $2 = '1'
		`

	scanner := bufio.NewScanner(strings.NewReader(log))
	logParser := NewPostgresLogParser(scanner)
	pgLog, err := logParser.Parse()
	assert.Nil(t, err)
	assert.Equal(t, pgLog.LogType, "bind", "they should be equal")
	assert.Equal(t, pgLog.StatementName, "<unnamed>", "they should be equal")
	assert.Equal(t, pgLog.Duration, time.Duration(139000), "they should be equal")
	assert.Equal(t, pgLog.Username, "postgres", "they should be equal")
	assert.Equal(t, pgLog.Database, "walle_test", "they should be equal")
}

func TestParsingShardPartition(t *testing.T) {
	log := `2021-01-11 15:25:36 EST [56193-3/9939-5708] postgres@baller_test LOG:  duration: 0.020 ms  execute <unnamed>: SELECT * FROM abacus101_shard6.transactions WHERE balance = 13.37`

	scanner := bufio.NewScanner(strings.NewReader(log))
	logParser := NewPostgresLogParser(scanner)
	pgLog, err := logParser.Parse()
	assert.Nil(t, err)
	assert.Equal(t, pgLog.Value, `SELECT * FROM abacus101_shard6.transactions WHERE balance = 13.37`)
	assert.Equal(t, pgLog.PartitionlessQuery, `SELECT * FROM transactions WHERE balance = 13.37`)
	assert.Equal(t, pgLog.ShardPartition, `abacus101_shard6`)
	scrubbedQuery := ScrubQuery(pgLog.Value)

	assert.Equal(t, `SELECT * FROM abacusN_shardN.transactions WHERE balance = N.N`, scrubbedQuery)
}

func TestParsingShardPartition2(t *testing.T) {
	log := `2021-01-11 15:25:36 EST [56193-3/9939-5708] postgres@baller_test LOG:  duration: 0.020 ms  execute <unnamed>: SELECT __user.id, __user.guid FROM "yolos_abacus_qa"."users" __user WHERE (__user.is_deleted = $1 OR __user.is_deleted is null) AND __user.guid IN ($2) AND __user.user_guid IN ($3) ORDER BY __user.id ASC LIMIT 1`

	scanner := bufio.NewScanner(strings.NewReader(log))
	logParser := NewPostgresLogParser(scanner)
	pgLog, err := logParser.Parse()
	assert.Nil(t, err)
	assert.Equal(t, pgLog.Value, `SELECT __user.id, __user.guid FROM "yolos_abacus_qa"."users" __user WHERE (__user.is_deleted = $1 OR __user.is_deleted is null) AND __user.guid IN ($2) AND __user.user_guid IN ($3) ORDER BY __user.id ASC LIMIT 1`)
	assert.Equal(t, pgLog.PartitionlessQuery, `SELECT __user.id, __user.guid FROM "users" __user WHERE (__user.is_deleted = $1 OR __user.is_deleted is null) AND __user.guid IN ($2) AND __user.user_guid IN ($3) ORDER BY __user.id ASC LIMIT 1`)
	assert.Equal(t, pgLog.ShardPartition, `yolos_abacus_qa`)
	scrubbedQuery := ScrubQuery(pgLog.Value)

	assert.Equal(t, `SELECT __user.id, __user.guid FROM "yolos_abacus_qa"."users" __user WHERE (__user.is_deleted = $N OR __user.is_deleted is null) AND __user.guid IN ($N) AND __user.user_guid IN ($N) ORDER BY __user.id ASC LIMIT N`, scrubbedQuery)
}

func TestParsingStatementMultiline(t *testing.T) {
	log := `2021-02-08 16:09:20 UTC [26820-153/0-3] postgres@bob1989_production LOG:  duration: 2723.044 ms  statement:
        WITH all_sequences AS (
         SELECT  pg_namespace.nspname as namespace
         ,       pg_class.relname as table
         ,       2^((pg_attribute.attlen*8)-1)-1 as upper_limit
         ,       pg_get_serial_sequence(pg_namespace.nspname || '.' || pg_class.relname, 'id') sequence_name
         FROM
           pg_attribute
         LEFT JOIN pg_class ON
           pg_class.oid = pg_attribute.attrelid
         LEFT JOIN pg_namespace ON
         `

	scanner := bufio.NewScanner(strings.NewReader(log))
	logParser := NewPostgresLogParser(scanner)
	pgLog, err := logParser.Parse()
	assert.Nil(t, err)
	assert.Equal(t, pgLog.LogType, "statement", "they should be equal")
	assert.Equal(t, pgLog.StatementName, "", "they should be equal")
	assert.Equal(t, pgLog.Duration, time.Duration(2723044000), "they should be equal")
	assert.Equal(t, pgLog.Username, "postgres", "they should be equal")
	assert.Equal(t, pgLog.Database, "bob1989_production", "they should be equal")
}

func TestParsingExecuteStatement(t *testing.T) {
	log := `2021-01-11 15:25:36 EST [56193-3/9939-5708] postgres@baller_test LOG:  duration: 0.020 ms  execute <unnamed>: SELECT 1 AS one FROM "borrower_applications" WHERE "borrower_applications"."confirmation_number" = $1 LIMIT $2
2021-01-11 15:25:36 EST [56193-3/9939-5709] postgres@baller_test DETAIL:  parameters: $1 = '6BE8-BC52-7545', $2 = '1'
		`

	scanner := bufio.NewScanner(strings.NewReader(log))
	logParser := NewPostgresLogParser(scanner)
	pgLog, err := logParser.Parse()
	assert.Nil(t, err)
	assert.Equal(t, pgLog.LogType, "execute", "they should be equal")
	assert.Equal(t, pgLog.StatementName, "<unnamed>", "they should be equal")
	assert.Equal(t, pgLog.Duration, time.Duration(20000), "they should be equal")
	assert.Equal(t, pgLog.Username, "postgres", "they should be equal")
	assert.Equal(t, pgLog.Database, "baller_test", "they should be equal")
	assert.Equal(t, pgLog.Value, `SELECT 1 AS one FROM "borrower_applications" WHERE "borrower_applications"."confirmation_number" = $1 LIMIT $2`, "they should be equal")
}

func TestParsingBrokenLog(t *testing.T) {
	log := `2021-01-11 15:25:36 EST [56193-3/9939-5708] postgres@walle_test LOG:  duration: 0.020 ms  execute<unnamed>: SELECT 1 AS one FROM "borrower_applications" WHERE "borrower_applications"."confirmation_number" = $1 LIMIT $2
2021-01-11 15:25:36 EST [56193-3/9939-5709] postgres@walle_test DETAIL:  parameters: $1 = '6BE8-BC52-7545', $2 = '1'
		`

	scanner := bufio.NewScanner(strings.NewReader(log))
	logParser := NewPostgresLogParser(scanner)
	pgLog, err := logParser.Parse()
	assert.Nil(t, err)
	assert.Equal(t, pgLog.LogType, "execute<unnamed>", "they should be equal")
	assert.Equal(t, pgLog.StatementName, "", "they should be equal")
	assert.Equal(t, pgLog.Duration, time.Duration(20000), "they should be equal")
	assert.Equal(t, pgLog.Username, "postgres", "they should be equal")
	assert.Equal(t, pgLog.Database, "walle_test", "they should be equal")
}

func TestParsingMatchingHeader(t *testing.T) {
	log := `2021-01-11 15:25:36 EST [56193-3/9939-5731] postgres@walle_test DETAIL:  parameters: $1 = '1220', $2 = '1'`

	scanner := bufio.NewScanner(strings.NewReader(log))
	logParser := NewPostgresLogParser(scanner)
	_, err := logParser.Parse()
	assert.Equal(t, err.Error(), "The parser could not derive query or plan info from the log line")
}

func TestScrubbingCanFilterStringsAndDigits(t *testing.T) {
	log := `2021-01-11 15:25:36 EST [56193-3/9939-5708] postgres@baller_test LOG:  duration: 0.020 ms  execute <unnamed>: SELECT * FROM transactions WHERE guid IN ('TRN-123', 'TRN-234') AND account_id IN (1, 2, 4, 5)`

	scanner := bufio.NewScanner(strings.NewReader(log))
	logParser := NewPostgresLogParser(scanner)
	pgLog, err := logParser.Parse()
	assert.Nil(t, err)
	assert.Equal(t, pgLog.Value, `SELECT * FROM transactions WHERE guid IN ('TRN-123', 'TRN-234') AND account_id IN (1, 2, 4, 5)`)

	scrubbedQuery := ScrubQuery(pgLog.Value)
	assert.Equal(t, `SELECT * FROM transactions WHERE guid IN ('XXX', 'XXX') AND account_id IN (N, N, N, N)`, scrubbedQuery)
}

func TestScrubbingCanFilterIdEquals(t *testing.T) {
	log := `2021-01-11 15:25:36 EST [56193-3/9939-5708] postgres@baller_test LOG:  duration: 0.020 ms  execute <unnamed>: SELECT * FROM transactions WHERE account_id = 5`

	scanner := bufio.NewScanner(strings.NewReader(log))
	logParser := NewPostgresLogParser(scanner)
	pgLog, err := logParser.Parse()
	assert.Nil(t, err)
	assert.Equal(t, pgLog.Value, `SELECT * FROM transactions WHERE account_id = 5`)

	scrubbedQuery := ScrubQuery(pgLog.Value)
	assert.Equal(t, `SELECT * FROM transactions WHERE account_id = N`, scrubbedQuery)
}

func TestScrubbingCanFilterDollarAmounts(t *testing.T) {
	log := `2021-01-11 15:25:36 EST [56193-3/9939-5708] postgres@baller_test LOG:  duration: 0.020 ms  execute <unnamed>: SELECT * FROM transactions WHERE balance = 13.37`

	scanner := bufio.NewScanner(strings.NewReader(log))
	logParser := NewPostgresLogParser(scanner)
	pgLog, err := logParser.Parse()
	assert.Nil(t, err)
	assert.Equal(t, pgLog.Value, `SELECT * FROM transactions WHERE balance = 13.37`)

	scrubbedQuery := ScrubQuery(pgLog.Value)
	assert.Equal(t, `SELECT * FROM transactions WHERE balance = N.N`, scrubbedQuery)
}

func TestScrubbingCanFilterSchemaName(t *testing.T) {
	log := `2021-01-11 15:25:36 EST [56193-3/9939-5708] postgres@baller_test LOG:  duration: 0.020 ms  execute <unnamed>: SELECT * FROM abacus101_shard6.transactions WHERE balance = 13.37`

	scanner := bufio.NewScanner(strings.NewReader(log))
	logParser := NewPostgresLogParser(scanner)
	pgLog, err := logParser.Parse()
	assert.Nil(t, err)
	assert.Equal(t, pgLog.Value, `SELECT * FROM abacus101_shard6.transactions WHERE balance = 13.37`)
	assert.Equal(t, pgLog.PartitionlessQuery, `SELECT * FROM transactions WHERE balance = 13.37`)

	scrubbedQuery := ScrubQuery(pgLog.Value)
	//TODO: Make this not strip the shard information.
	assert.Equal(t, `SELECT * FROM abacusN_shardN.transactions WHERE balance = N.N`, scrubbedQuery)
}
