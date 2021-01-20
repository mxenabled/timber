package main

import (
	"bufio"
	"strings"
	"testing"

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
}
