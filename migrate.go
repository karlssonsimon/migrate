package migrate

import (
	"fmt"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"log"
)

type Migration interface {
	// Name of the migration (to keep track if it has been applied)
	Name() string

	// Sql as string to perform upgorade
	SqlUp() string
}

const (
	defaultTable = "migrations"
)

type M struct {
	migrationTable string
}

func NewDefaultMigrate() *M {
	return NewMigrate(defaultTable)
}

func NewMigrate(migrationTableName string) *M {
	return &M{migrationTable: migrationTableName}
}

// Migrations are applied in the same order that they are passed
// when an error is encountered the migration is aborted at the current stage
func (mig M) Run(db *sqlx.DB, migrations []Migration) {
	mig.initMigrationTable(db)

	appliedMigrations := mig.getAppliedMigrations(db)

	var completedMigrations []Migration
	for _, m := range migrations {
		if !contains(m.Name(), appliedMigrations) {
			log.Printf("Executing migration: [%s]...\n", m.Name())
			_, err := db.Exec(m.SqlUp())

			if err != nil {
				log.Printf("Error when executing migation [%s]:%v\n", m.Name(), err)
				log.Printf("Aborting the current migration...\n")
				break
			}

			completedMigrations = append(completedMigrations, m)
		}
	}

	if len(completedMigrations) > 0 {
		mig.saveMigrations(db, completedMigrations)
		log.Printf("Successfully applied %d migration(s) to the database", len(completedMigrations))
	}
}

func (mig M) initMigrationTable(db *sqlx.DB) {
	if !tableExists(db, mig.migrationTable) {
		var migrationSchema = fmt.Sprintf("CREATE TABLE %s (migration text UNIQUE);", mig.migrationTable)
		db.MustExec(migrationSchema)
	}
}

func (mig M) getAppliedMigrations(db *sqlx.DB) []string {
	sql := fmt.Sprintf(`SELECT migration FROM %s ORDER BY migration DESC;`, mig.migrationTable)

	rows, err := db.Query(sql)
	if err != nil {
		log.Fatalf("Couldn't load migrations: %v", err)
	}

	var m []string
	for rows.Next() {
		var mstr string
		err := rows.Scan(&mstr)
		if err != nil {
			log.Fatalf("Couldn't scan migrations: %v", err)
		}

		m = append(m, mstr)
	}

	return m
}

func (mig M) saveMigrations(db *sqlx.DB, completedMigrations []Migration) {
	sql := fmt.Sprintf("INSERT INTO %s(migration) VALUES($1);", mig.migrationTable)

	for _, m := range completedMigrations {
		migrationName := m.Name()
		_, err := db.Exec(sql, migrationName)

		if err != nil {
			log.Printf("Failed to update migration table with failed migrations %s:%v\n", m.Name(), err)
		}
	}
}

func tableExists(db *sqlx.DB, name string) bool {
	var result bool
	var sql = `SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = $1);`
	err := db.QueryRow(sql, name).Scan(&result)

	if err != nil {
		log.Printf("Couldn't check if table '%s' exists: %v\n", name, err)
	}

	return result
}

func contains(migration string, migrations []string) bool {
	for _, m := range migrations {
		if m == migration {
			return true
		}
	}

	return false
}
