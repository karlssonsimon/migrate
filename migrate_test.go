package migrate

import (
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

var db = sqlx.MustConnect("postgres", "user=user password=safe dbname=migrations sslmode=disable port=5435")

type testMigration struct {
	n string
	s string
}

func (t testMigration) Name() string {
	return t.n
}

func (t testMigration) SqlUp() string {
	return t.s
}

func getRandomTableName() string {
	return "test_migration_" + strings.Replace(uuid.NewV4().String(), "-", "_", -1)
}

func getTestMigrate() (*M, string) {
	table := getRandomTableName()
	m := NewMigrate(table)
	m.initMigrationTable(db)
	return NewMigrate(table), table
}

func TestSuccessfulRun(t *testing.T) {
	table := getRandomTableName()
	m := NewMigrate(table)

	t1 := getRandomTableName()
	sql1 := fmt.Sprintf("CREATE TABLE %s(random text)", t1)
	m1 := testMigration{n: "001", s: sql1}

	t2 := getRandomTableName()
	sql2 := fmt.Sprintf("CREATE TABLE %s(random text)", t2)
	m2 := testMigration{n: "002", s: sql2}

	migrations := []Migration{m1, m2}
	m.Run(db, migrations)

	sql := "SELECT COUNT(1) FROM information_schema.tables WHERE table_name IN ($1, $2);"
	var count int
	_ = db.Get(&count, sql, t1, t2)

	assert.Equal(t, 2, count)
}

func TestInitMigrationTable(t *testing.T) {
	mig, table := getTestMigrate()
	mig.initMigrationTable(db)

	sql := "SELECT COUNT(1) FROM information_schema.tables WHERE table_name = $1;"
	var count int
	_ = db.Get(&count, sql, table)

	assert.Equal(t, 1, count)
}

func TestGetAppliedMigrations(t *testing.T) {
	m1 := uuid.NewV4().String()
	m2 := uuid.NewV4().String()
	mig, table := getTestMigrate()

	sql := fmt.Sprintf("INSERT INTO %s(migration) VALUES($1), ($2);", table)
	db.MustExec(sql, m1, m2)

	applied := mig.getAppliedMigrations(db)

	assert.ElementsMatch(t, []string{m1, m2}, applied)
}

func TestSaveMigrations(t *testing.T) {
	m1 := testMigration{n: uuid.NewV4().String()}
	m2 := testMigration{n: uuid.NewV4().String()}

	migrations := []Migration{m1, m2}

	mig, table := getTestMigrate()
	mig.saveMigrations(db, migrations)

	sql := fmt.Sprintf("SELECT COUNT(1) FROM %s WHERE migration IN ($1, $2);", table)
	var count int
	_ = db.Get(&count, sql, m1.n, m2.n)

	assert.Equal(t, 2, count)
}

func TestTableExists(t *testing.T) {
	tableName := "test_" + strings.Replace(uuid.NewV4().String(), "-", "_", -1)
	sql := fmt.Sprintf("CREATE TABLE %s(col text);", tableName)
	db.MustExec(sql)
	assert.True(t, tableExists(db, tableName))
}

func TestTableNotExists(t *testing.T) {
	assert.False(t, tableExists(db, uuid.NewV4().String()))
}

func TestContains(t *testing.T) {
	migrations := []string{"aa", "bb"}

	assert.True(t, contains("aa", migrations))
	assert.False(t, contains("aA", migrations))
	assert.False(t, contains("cc", migrations))
}
