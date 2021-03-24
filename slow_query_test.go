package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParsingDerivedFromValue(t *testing.T) {
	value := `SELECT * FROM abacus101_shard6.transactions WHERE balance = '13.37'`
	shardName, shardlessQuery := DerivedValues(value)

	assert.Equal(t, shardlessQuery, `SELECT * FROM transactions WHERE balance = '13.37'`)
	assert.Equal(t, shardName, `abacus101_shard6`)
	scrubbedQuery := ScrubQuery(value)

	assert.Equal(t, `SELECT * FROM abacus101_shard6.transactions WHERE balance = 'xxx'`, scrubbedQuery)
}

func TestParsingDerivedFromValueGolangFormat(t *testing.T) {
	value := `SELECT __user.id, __user.guid FROM "yolos_qa"."users" __user WHERE (__user.is_deleted = $1 OR __user.is_deleted is null) AND __user.guid IN ($2) AND __user.user_guid IN ($3) ORDER BY __user.id ASC LIMIT 1`
	shardName, shardlessQuery := DerivedValues(value)

	assert.Equal(t, shardlessQuery, `SELECT __user.id, __user.guid FROM "users" __user WHERE (__user.is_deleted = $1 OR __user.is_deleted is null) AND __user.guid IN ($2) AND __user.user_guid IN ($3) ORDER BY __user.id ASC LIMIT 1`)
	assert.Equal(t, shardName, `yolos_qa`)
	scrubbedQuery := ScrubQuery(value)

	assert.Equal(t, `SELECT __user.id, __user.guid FROM "yolos_qa"."users" __user WHERE (__user.is_deleted = $1 OR __user.is_deleted is null) AND __user.guid IN ($2) AND __user.user_guid IN ($3) ORDER BY __user.id ASC LIMIT 1`, scrubbedQuery)
}

func TestParsingDerivedFromValueRubyFormat(t *testing.T) {
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

func TestFloatNotFiltered(t *testing.T) {
	value := `SELECT * FROM transactions WHERE amount = 5.15`

	shardName, shardlessQuery := DerivedValues(value)
	assert.Equal(t, "", shardName)
	assert.Equal(t, `SELECT * FROM transactions WHERE amount = 5.15`, shardlessQuery)
}

func TestParsingDerivedWhenNoShardCaseInsensitive(t *testing.T) {
	value := `select * From transactions t where t.id = 1`

	shardName, shardlessQuery := DerivedValues(value)
	assert.Equal(t, "", shardName)
	assert.Equal(t, `select * From transactions t where t.id = 1`, shardlessQuery)
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

func TestParsingSimpleShard(t *testing.T) {
	value := `select * from boys.to_mens`

	shardName, shardlessQuery := DerivedValues(value)
	assert.Equal(t, "boys", shardName)
	assert.Equal(t, `select * from to_mens`, shardlessQuery)
}

func TestDateTimeWhitelist(t *testing.T) {
	value := `'2021-03-13 11:45:00.000000'` //correct format
	wv := isWhitelisted(value)
	assert.True(t, wv)

	value2 := `'2021-03-13 6:45:00.000000'` //incorrect datetime format
	wv2 := isWhitelisted(value2)
	assert.False(t, wv2)
}

func TestGuidWhitelist(t *testing.T) {
	value := `'USR-f164af58-bb51-47ed-aa35-368ae3f46648'` //correct format
	wv := isWhitelisted(value)
	assert.True(t, wv)

	value2 := `'USR-f164af58-bb51-aa35-368ae3f46648'` //incorrect guid format
	wv2 := isWhitelisted(value2)
	assert.False(t, wv2)

	value3 := `'This is my guid USR-f164af58-bb54-47ed-aa35-368ae3f46648'` // Valid GUID but not issolated so string will be filtered to 'xxx'
	wv3 := isWhitelisted(value3)
	assert.False(t, wv3)
}

func TestBoolWhitelist(t *testing.T) {
	value := `'t'` //correct format
	wv := isWhitelisted(value)
	assert.True(t, wv)

	value1 := `'f'` //correct format
	wv1 := isWhitelisted(value1)
	assert.True(t, wv1)

	value2 := `'tf'` //incorrect bool format
	wv2 := isWhitelisted(value2)
	assert.False(t, wv2)

	value3 := `'faulty'` //incorrect bool format, still has f or t in string
	wv3 := isWhitelisted(value3)
	assert.False(t, wv3)
}

func TestParsingFullQuery(t *testing.T) {
	value := `SELECT  "abacus3_qa"."transactions"."guid" FROM "abacus3_qa"."transactions" WHERE ("abacus3_qa"."transactions"."date" BETWEEN '2021-03-13 11:45:00.000000' AND '2021-03-13 12:15:00.000000') AND "abacus3_qa"."transactions"."account_id" = 252641 AND "abacus3_qa"."transactions"."amount" = '7.82' AND "abacus3_qa"."transactions"."is_deleted" = 'f' AND "abacus3_qa"."transactions"."status" = 1 AND "abacus3_qa"."transactions"."transaction_type" = 2 AND "abacus3_qa"."transactions"."user_guid" = 'USR-f164af58-bb51-47ed-aa35-368ae3f46648' AND "abacus3_qa"."transactions"."merchant_guid" IS NULL AND "abacus3_qa"."transactions"."parent_id" IS NULL AND "abacus3_qa"."transactions"."description" = 'Children''s Hospital'  ORDER BY "abacus3_qa"."transactions"."id" ASC LIMIT 10`

	shardName, shardlessQuery := DerivedValues(value)
	scrubbedQuery := ScrubQuery(shardlessQuery)
	assert.Equal(t, "abacus3_qa", shardName)

	assert.Equal(t, `SELECT  "transactions"."guid" FROM "transactions" WHERE ("transactions"."date" BETWEEN '2021-03-13 11:45:00.000000' AND '2021-03-13 12:15:00.000000') AND "transactions"."account_id" = 252641 AND "transactions"."amount" = '7.82' AND "transactions"."is_deleted" = 'f' AND "transactions"."status" = 1 AND "transactions"."transaction_type" = 2 AND "transactions"."user_guid" = 'USR-f164af58-bb51-47ed-aa35-368ae3f46648' AND "transactions"."merchant_guid" IS NULL AND "transactions"."parent_id" IS NULL AND "transactions"."description" = 'Children''s Hospital'  ORDER BY "transactions"."id" ASC LIMIT 10`, shardlessQuery)

	assert.Equal(t, `SELECT  "transactions"."guid" FROM "transactions" WHERE ("transactions"."date" BETWEEN '2021-03-13 11:45:00.000000' AND '2021-03-13 12:15:00.000000') AND "transactions"."account_id" = 252641 AND "transactions"."amount" = 'xxx' AND "transactions"."is_deleted" = 'f' AND "transactions"."status" = 1 AND "transactions"."transaction_type" = 2 AND "transactions"."user_guid" = 'USR-f164af58-bb51-47ed-aa35-368ae3f46648' AND "transactions"."merchant_guid" IS NULL AND "transactions"."parent_id" IS NULL AND "transactions"."description" = 'xxx''xxx'  ORDER BY "transactions"."id" ASC LIMIT 10`, scrubbedQuery)
}
