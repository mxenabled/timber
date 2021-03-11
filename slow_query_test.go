package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParsingDerivedFromValue(t *testing.T) {
	value := `SELECT * FROM abacus101_shard6.transactions WHERE balance = 13.37`
	shardPartition, partitionlessQuery := derivedValues(value)

	assert.Equal(t, partitionlessQuery, `SELECT * FROM transactions WHERE balance = 13.37`)
	assert.Equal(t, shardPartition, `abacus101_shard6`)
	scrubbedQuery := ScrubQuery(value)

	assert.Equal(t, `SELECT * FROM abacusN_shardN.transactions WHERE balance = N.N`, scrubbedQuery)
}

func TestParsingDerivedFromValue2(t *testing.T) {
	value := `SELECT __user.id, __user.guid FROM "yolos_abacus_qa"."users" __user WHERE (__user.is_deleted = $1 OR __user.is_deleted is null) AND __user.guid IN ($2) AND __user.user_guid IN ($3) ORDER BY __user.id ASC LIMIT 1`
	shardPartition, partitionlessQuery := derivedValues(value)

	assert.Equal(t, partitionlessQuery, `SELECT __user.id, __user.guid FROM "users" __user WHERE (__user.is_deleted = $1 OR __user.is_deleted is null) AND __user.guid IN ($2) AND __user.user_guid IN ($3) ORDER BY __user.id ASC LIMIT 1`)
	assert.Equal(t, shardPartition, `yolos_abacus_qa`)
	scrubbedQuery := ScrubQuery(value)

	assert.Equal(t, `SELECT __user.id, __user.guid FROM "yolos_abacus_qa"."users" __user WHERE (__user.is_deleted = $N OR __user.is_deleted is null) AND __user.guid IN ($N) AND __user.user_guid IN ($N) ORDER BY __user.id ASC LIMIT N`, scrubbedQuery)
}
