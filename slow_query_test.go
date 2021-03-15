package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParsingDerivedFromValue(t *testing.T) {
	value := `SELECT * FROM abacus101_shard6.transactions WHERE balance = 13.37`
	shardName, shardlessQuery := DerivedValues(value)

	assert.Equal(t, shardlessQuery, `SELECT * FROM transactions WHERE balance = 13.37`)
	assert.Equal(t, shardName, `abacus101_shard6`)
	scrubbedQuery := ScrubQuery(value)

	assert.Equal(t, `SELECT * FROM abacusN_shardN.transactions WHERE balance = N.N`, scrubbedQuery)
}

func TestParsingDerivedFromValue2(t *testing.T) {
	value := `SELECT __user.id, __user.guid FROM "yolos_qa"."users" __user WHERE (__user.is_deleted = $1 OR __user.is_deleted is null) AND __user.guid IN ($2) AND __user.user_guid IN ($3) ORDER BY __user.id ASC LIMIT 1`
	shardName, shardlessQuery := DerivedValues(value)

	assert.Equal(t, shardlessQuery, `SELECT __user.id, __user.guid FROM "users" __user WHERE (__user.is_deleted = $1 OR __user.is_deleted is null) AND __user.guid IN ($2) AND __user.user_guid IN ($3) ORDER BY __user.id ASC LIMIT 1`)
	assert.Equal(t, shardName, `yolos_qa`)
	scrubbedQuery := ScrubQuery(value)

	assert.Equal(t, `SELECT __user.id, __user.guid FROM "yolos_qa"."users" __user WHERE (__user.is_deleted = $N OR __user.is_deleted is null) AND __user.guid IN ($N) AND __user.user_guid IN ($N) ORDER BY __user.id ASC LIMIT N`, scrubbedQuery)
}

func TestParsingDerivedFromValue3(t *testing.T) {
	value := `SELECT  "abacustody19_qa"."monthly_cash_flow_profiles".* FROM "abacustody19_qa"."monthly_cash_flow_profiles" WHERE ("abacustody19_qa"."monthly_cash_flow_profiles"."is_deleted" IN ('t', 'f') OR "abacustody19_qa"."monthly_cash_flow_profiles"."is_deleted" IS NULL) AND "abacustody19_qa"."monthly_cash_flow_profiles"."user_guid" = 'USR-4f724653-e88d-457b-b151-8c32fb3c51c2'  ORDER BY "abacustody19_qa"."monthly_cash_flow_profiles"."id" ASC LIMIT 25 OFFSET 0`
	shardName, shardlessQuery := DerivedValues(value)

	assert.Equal(t, shardlessQuery, `SELECT  "monthly_cash_flow_profiles".* FROM "monthly_cash_flow_profiles" WHERE ("monthly_cash_flow_profiles"."is_deleted" IN ('t', 'f') OR "monthly_cash_flow_profiles"."is_deleted" IS NULL) AND "monthly_cash_flow_profiles"."user_guid" = 'USR-4f724653-e88d-457b-b151-8c32fb3c51c2'  ORDER BY "monthly_cash_flow_profiles"."id" ASC LIMIT 25 OFFSET 0`)
	assert.Equal(t, shardName, `abacustody19_qa`)
}

func TestParsingDerivedWhenNoShard(t *testing.T) {
	value := `SELECT * FROM transactions WHERE account_id = 5`

	shardName, shardlessQuery := DerivedValues(value)
	assert.Equal(t, "", shardName)
	assert.Equal(t, `SELECT * FROM transactions WHERE account_id = 5`, shardlessQuery)
}

func TestParsingDerivedMultiline(t *testing.T) {
	value := `SELECT *
FROM 
  yolos.brolos
WHERE
  yolos.brolos.id = 1`

	shardName, shardlessQuery := DerivedValues(value)
	assert.Equal(t, "yolos", shardName)
	assert.Equal(t, `SELECT *
FROM 
  brolos
WHERE
  brolos.id = 1`, shardlessQuery)
}

func TestParsingDerivedWhenNoShardCaseInsensitive2(t *testing.T) {
	value := `select * From transactions t where t.id = 1`

	shardName, shardlessQuery := DerivedValues(value)
	assert.Equal(t, "", shardName)
	assert.Equal(t, `select * From transactions t where t.id = 1`, shardlessQuery)
}

func TestParsingDerivedWhenNoShard3(t *testing.T) {
	value := `select * from boys.to_mens`

	shardName, shardlessQuery := DerivedValues(value)
	assert.Equal(t, "boys", shardName)
	assert.Equal(t, `select * from to_mens`, shardlessQuery)
}
