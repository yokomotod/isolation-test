package test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	_ "github.com/mattn/go-sqlite3"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/yokomotod/zakodb/pkg/transactonstest"
)

func setupMySQL(ctx context.Context) (testcontainers.Container, driver.Connector, error) {
	req := testcontainers.ContainerRequest{
		Image: "mysql:8.0.31",
		Env: map[string]string{
			"MYSQL_DATABASE": "test",
			// "MYSQL_USER":                 "user",
			// "MYSQL_PASSWORD":             "password",
			"MYSQL_ALLOW_EMPTY_PASSWORD": "yes",
		},
		ExposedPorts: []string{"3306/tcp"},
		WaitingFor: wait.ForSQL("3306", "mysql", func(host string, port nat.Port) string {
			cfg := mysql.NewConfig()
			cfg.Net = "tcp"
			cfg.Addr = net.JoinHostPort(host, port.Port())
			cfg.DBName = "test"
			cfg.User = "root"
			// cfg.Passwd = "password"
			return cfg.FormatDSN()
		}),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, nil, err
	}

	port, err := container.MappedPort(ctx, "3306")
	if err != nil {
		return nil, nil, err
	}

	cfg := mysql.NewConfig()
	cfg.Net = "tcp"
	cfg.Addr = net.JoinHostPort("localhost", port.Port())
	cfg.DBName = "test"
	cfg.User = "root"
	// cfg.Passwd = "password"

	connector, err := mysql.NewConnector(cfg)
	if err != nil {
		return nil, nil, err
	}

	return container, connector, nil
}

func setupPostgreSQL(ctx context.Context) (testcontainers.Container, driver.Connector, error) {
	req := testcontainers.ContainerRequest{
		Image: "postgres:15.0",
		Env: map[string]string{
			// "MYSQL_DATABASE": "test",
			// "MYSQL_USER":                 "user",
			"POSTGRES_PASSWORD": "postgres",
			// "postgres": "yes",
		},
		ExposedPorts: []string{"5432/tcp"},
		WaitingFor: wait.ForSQL("5432", "pgx", func(host string, port nat.Port) string {
			return fmt.Sprintf("postgres://postgres:postgres@127.0.0.1:%d/postgres", port.Int())
		}),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, nil, err
	}

	port, err := container.MappedPort(ctx, "5432")
	if err != nil {
		return nil, nil, err
	}

	config, err := pgx.ParseConfig(fmt.Sprintf("postgres://postgres:postgres@127.0.0.1:%d/postgres", port.Int()))
	if err != nil {
		return nil, nil, err
	}
	connector := stdlib.GetConnector(*config)

	return container, connector, nil
}

// func TestMySQL(t *testing.T) {
// 	t.SkipNow()
// 	ctx := context.Background()
// 	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(3*time.Second))
// 	defer cancel()
// 	// container, connector, err := setupMySQL(ctx)
// 	// if err != nil {
// 	// 	t.Fatal(err)
// 	// }
// 	// defer container.Terminate(ctx)
// 	cfg := mysql.NewConfig()
// 	// cfg.Net = "tcp"
// 	// cfg.Addr = net.JoinHostPort("127.0.0.1", 3306)
// 	cfg.DBName = "test"
// 	cfg.User = "root"
// 	// cfg.Passwd = "password"

// 	connector, err := mysql.NewConnector(cfg)
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// 	db := sql.OpenDB(connector)
// 	defer db.Close()

// 	conn, err := db.Conn(ctx)
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// 	_, err = conn.ExecContext(ctx, "SET GLOBAL TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// 	_, err = conn.ExecContext(ctx, "DROP TABLE IF EXISTS foo")
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	_, err = conn.ExecContext(ctx, "CREATE TABLE foo (id INT, value INT)")
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	_, err = conn.ExecContext(ctx, "INSERT INTO foo VALUES (1, 2), (3, 4)")
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// 	txs := [][]string{
// 		{
// 			"UPDATE foo SET value = 20 WHERE id = 1",
// 			"UPDATE foo SET value = 40 WHERE id = 3",
// 		},
// 		{
// 			"UPDATE foo SET value = 200 WHERE id = 1",
// 			"UPDATE foo SET value = 400 WHERE id = 3",
// 		},
// 	}

// 	wantStarts := []string{"0:0", "1:0", "0:1", "1:1"}
// 	wantEnds := []string{"0:0", "0:1", "1:0", "1:1"}

// 	transactonstest.RunTransactionsTest(t, ctx, db, txs, wantStarts, wantEnds)
// }

// func TestPostgreSQL(t *testing.T) {
// 	t.SkipNow()
// 	ctx := context.Background()

// 	container, connector, err := setupPostgreSQL(ctx)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	defer container.Terminate(ctx)

// 	db := sql.OpenDB(connector)
// 	defer db.Close()

// 	conn, err := db.Conn(ctx)
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// 	// _, err = conn.ExecContext(ctx, "SET GLOBAL TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
// 	// if err != nil {
// 	// 	t.Fatal(err)
// 	// }

// 	_, err = conn.ExecContext(ctx, "CREATE TABLE foo (id INT, value INT)")
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	_, err = conn.ExecContext(ctx, "INSERT INTO foo VALUES (1, 2), (3, 4)")
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// 	txs := [][]string{
// 		{
// 			"UPDATE foo SET value = 20 WHERE id = 1",
// 			"UPDATE foo SET value = 40 WHERE id = 3",
// 		},
// 		{
// 			"UPDATE foo SET value = 200 WHERE id = 1",
// 			"UPDATE foo SET value = 400 WHERE id = 3",
// 		},
// 	}

// 	wantStarts := []string{"0:0", "1:0", "0:1", "1:1"}
// 	wantEnds := []string{"0:0", "0:1", "1:0", "1:1"}

// 	transactonstest.RunTransactionsTest(t, ctx, db, txs, wantStarts, wantEnds)
// }

func TestSQLite(t *testing.T) {
	t.SkipNow()
	ctx := context.Background()

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}

	_, err = conn.ExecContext(ctx, "PRAGMA read_uncommitted = true")
	if err != nil {
		t.Fatal(err)
	}
}

func openDB(database string) (*sql.DB, error) {
	switch database {
	case "mysql":
		return sql.Open("mysql", "root@/test")
	case "postgres":
		return sql.Open("pgx", "postgres://postgres:postgres@127.0.0.1:5432/postgres")
	case "sqlite3":
		// return sql.Open("sqlite3", "file::memory:?cache=shared&_busy_timeout=5000")
		// return sql.Open("sqlite3", "file::memory:?cache=shared")
		return sql.Open("sqlite3", "sqlite3.db")
	default:
		panic(fmt.Errorf("unknown database: %s", database))
	}
}

func setTransactionIsolationLevel(database string, level string) string {
	switch database {
	case "mysql":
		fallthrough
	case "postgres":
		return fmt.Sprintf("SET TRANSACTION ISOLATION LEVEL %s", level)
	case "sqlite3":
		if level == "READ UNCOMMITTED" {
			return "PRAGMA read_uncommitted = true"
		}

		return "SELECT 1" // dummy
	default:
		panic(fmt.Errorf("unknown database: %s", database))
	}
}

// func begin(database string) string {
// 	switch database {
// 	case "mysql":
// 		fallthrough
// 	case "postgres":
// 		return "BEGIN" // or "START TRANSACTION"
// 	case "sqlite3":
// 		// "START TRANSACTION" is not supported on sqlite3
// 		// "BEGIN" = "BEGIN DEFERRED" returns "database table is locked" error instead of wait
// 		return "BEGIN"
// 	default:
// 		panic(fmt.Errorf("unknown database: %s", database))
// 	}
// }

// docker run -d -e MYSQL_DATABASE=test -e MYSQL_ALLOW_EMPTY_PASSWORD=yes -p 3306:3306 mysql:8.0.31
// docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=postgres postgres:15.0

func newNullInt(value int64) *sql.NullInt64 {
	return &sql.NullInt64{Int64: value, Valid: true}
}

func Test(t *testing.T) {
	var databases = []string{"mysql", "postgres", "sqlite3"}

	type test struct {
		database   string
		name       string
		txs        [][]transactonstest.Query
		wantStarts []string
		wantEnds   []string
		skip       bool
	}
	tests := make([]test, 0)

	for _, database := range databases {

		tests = append(tests, []test{
			//
			// dirty write
			//
			{
				database: database,
				name:     "lock w/o transaction",
				txs: [][]transactonstest.Query{
					{
						{Query: "UPDATE foo SET value = 20 WHERE id = 1"},
						{Query: "UPDATE foo SET value = 40 WHERE id = 3"},
					},
					{
						{Query: "UPDATE foo SET value = 200 WHERE id = 1"},
						{Query: "UPDATE foo SET value = 400 WHERE id = 3"},
					},
				},
				wantStarts: []string{"0:0", "1:0", "0:1", "1:1"},
				wantEnds:   []string{"0:0", "1:0", "0:1", "1:1"},
			},
			{
				database: database,
				name:     "lock w/ transaction",
				txs: [][]transactonstest.Query{
					{
						// {Query: setTransactionIsolationLevel(database)},
						{Query: "BEGIN"},
						{Query: "UPDATE foo SET value = 20 WHERE id = 1"},
						{Query: "UPDATE foo SET value = 40 WHERE id = 3"},
						{Query: "COMMIT"},
					},
					{
						// {Query: setTransactionIsolationLevel(database)},
						{Query: "BEGIN"},
						{Query: "UPDATE foo SET value = 200 WHERE id = 1"},
						{Query: "UPDATE foo SET value = 400 WHERE id = 3"},
						{Query: "COMMIT"},
					},
				},
				wantStarts: []string{"0:0", "1:0", "0:1", "1:1", "0:2", "0:3", "1:2", "1:3"},
				wantEnds:   []string{"0:0", "1:0", "0:1", "0:2", "0:3", "1:1", "1:2", "1:3"},
				// wantStarts: []string{"0:0", "1:0", "0:1", "1:1", "0:2", "1:2", "0:3", "0:4", "1:3", "1:4"},
				// wantEnds:   []string{"0:0", "1:0", "0:1", "1:1", "0:2", "0:3", "0:4", "1:2", "1:3", "1:4"},
			},

			//
			// dirty read
			//
			{
				database: database,
				name:     "select w/o transaction",
				txs: [][]transactonstest.Query{
					{
						{Query: "UPDATE foo SET value = 20 WHERE id = 1"},
					},
					{
						{Query: "SELECT value FROM foo WHERE id = 1", Want: newNullInt(20)}, // dirty read
					},
				},
				wantStarts: []string{"0:0", "1:0"},
				// wantEnds:   []string{"0:0", "1:0"}, // mysql fails, maybe too fast to select
			},
			{
				database: database,
				name:     "select w/ transaction before commit, READ COMMITTED",
				txs: [][]transactonstest.Query{
					{
						{Query: setTransactionIsolationLevel(database, "READ COMMITTED")},
						{Query: "BEGIN"},
						{Query: "UPDATE foo SET value = 20 WHERE id = 1"},
						{Query: "COMMIT"},
					},
					{
						{Query: setTransactionIsolationLevel(database, "READ COMMITTED")},
						{Query: "BEGIN"},
						{Query: "SELECT value FROM foo WHERE id = 1", Want: newNullInt(2)}, // no dirty read
						{Query: "ROLLBACK"},
					},
				},
				wantStarts: []string{"0:0", "1:0", "0:1", "1:1", "0:2", "1:2", "0:3", "1:3"},
				wantEnds:   []string{"0:0", "1:0", "0:1", "1:1", "0:2", "1:2", "0:3", "1:3"},
				skip:       func(d string) bool { return d == "sqlite3" }(database),
			},
			{
				database: database,
				name:     "select w/ transaction before commit, READ UNCOMMITTED",
				txs: [][]transactonstest.Query{
					{
						{Query: setTransactionIsolationLevel(database, "READ UNCOMMITTED")},
						{Query: "BEGIN"},
						{Query: "UPDATE foo SET value = 20 WHERE id = 1"},
						{Query: "COMMIT"},
					},
					{
						{Query: setTransactionIsolationLevel(database, "READ UNCOMMITTED")},
						{Query: "BEGIN"},
						{Query: "SELECT value FROM foo WHERE id = 1", Want: func(d string) *sql.NullInt64 {
							if d == "postgres" {
								return newNullInt(2) // postgres's READ UNCOMMITTED is READ COMMITTED, so no dirty read
							}
							if d == "sqlite3" {
								return newNullInt(2) // no dirty read ?
							}
							return newNullInt(20)
						}(database)},
						{Query: "ROLLBACK"},
					},
				},
				wantStarts: []string{"0:0", "1:0", "0:1", "1:1", "0:2", "1:2", "0:3", "1:3"},
				wantEnds: func(d string) []string {
					if d == "sqlite3" {
						// tx0:COMMIT will be locked
						return []string{"0:0", "1:0", "0:1", "1:1", "0:2", "1:2", "1:3", "0:3"}
					}
					return []string{"0:0", "1:0", "0:1", "1:1", "0:2", "1:2", "0:3", "1:3"}
				}(database),
			},
			{
				database: database,
				name:     "select w/ transaction before commit, REPEATABLE READ",
				txs: [][]transactonstest.Query{
					{
						{Query: setTransactionIsolationLevel(database, "REPEATABLE READ")},
						{Query: "BEGIN"},
						{Query: "UPDATE foo SET value = 20 WHERE id = 1"},
						{Query: "COMMIT"},
					},
					{
						{Query: setTransactionIsolationLevel(database, "REPEATABLE READ")},
						{Query: "BEGIN"},
						{Query: "SELECT value FROM foo WHERE id = 1", Want: newNullInt(2)},
						{Query: "ROLLBACK"},
					},
				},
				wantStarts: []string{"0:0", "1:0", "0:1", "1:1", "0:2", "1:2", "0:3", "1:3"},
				wantEnds:   []string{"0:0", "1:0", "0:1", "1:1", "0:2", "1:2", "0:3", "1:3"},
				skip:       func(d string) bool { return d == "sqlite3" }(database),
			},
			{
				database: database,
				name:     "select w/ transaction before commit",
				txs: [][]transactonstest.Query{
					{
						{Query: setTransactionIsolationLevel(database, "SERIALIZABLE")},
						{Query: "BEGIN"},
						{Query: "UPDATE foo SET value = 20 WHERE id = 1"},
						{Query: "COMMIT"},
					},
					{
						{Query: setTransactionIsolationLevel(database, "SERIALIZABLE")},
						{Query: "BEGIN"},
						{Query: "SELECT value FROM foo WHERE id = 1", Want: func(d string) *sql.NullInt64 {
							if d == "mysql" {
								return newNullInt(20) // locked and read committed value
							}

							return newNullInt(2)
						}(database)},
						{Query: "ROLLBACK"},
					},
				},
				wantStarts: []string{"0:0", "1:0", "0:1", "1:1", "0:2", "1:2", "0:3", "1:3"},
				wantEnds: func(d string) []string {
					if d == "sqlite3" {
						// tx0:COMMIT will be locked
						return []string{"0:0", "1:0", "0:1", "1:1", "0:2", "1:2", "1:3", "0:3"}
					}
					if d == "mysql" {
						// tx1: SELECT will be locked
						return []string{"0:0", "1:0", "0:1", "1:1", "0:2", "0:3", "1:2", "1:3"}
					}
					return []string{"0:0", "1:0", "0:1", "1:1", "0:2", "1:2", "0:3", "1:3"}
				}(database),
			},

			//
			// read skew
			//
			{
				database: database,
				name:     "select w/ transaction after commit, REPEATABLE READ",
				txs: [][]transactonstest.Query{
					{
						{Query: setTransactionIsolationLevel(database, "REPEATABLE READ")},
						{Query: "SELECT 1"},
						{Query: "SELECT 1"},
						{Query: "UPDATE foo SET value = 20 WHERE id = 1"},
					},
					{
						{Query: setTransactionIsolationLevel(database, "REPEATABLE READ")},
						{Query: "BEGIN"},
						{Query: "SELECT value FROM foo WHERE id = 1", Want: newNullInt(2)},
						{Query: "SELECT value FROM foo WHERE id = 1", Want: func(d string) *sql.NullInt64 {
							if d == "postgres" {
								return newNullInt(20) // postgres can't block read skew ?
							}
							return newNullInt(2)
						}(database)},
						{Query: "ROLLBACK"},
					},
				},
				wantStarts: []string{"0:0", "1:0", "0:1", "1:1", "0:2", "1:2", "0:3", "1:3", "1:4"},
				wantEnds:   []string{"0:0", "1:0", "0:1", "1:1", "0:2", "1:2", "0:3", "1:3", "1:4"}, // no lock
				skip:       func(d string) bool { return d == "sqlite3" }(database),
			},
			{
				database: database,
				name:     "select w/ transaction after commit, READ UNCOMMITTED",
				txs: [][]transactonstest.Query{
					{
						{Query: setTransactionIsolationLevel(database, "READ UNCOMMITTED")},
						{Query: "SELECT 1"},
						{Query: "SELECT 1"},
						{Query: "UPDATE foo SET value = 20 WHERE id = 1"},
					},
					{
						{Query: setTransactionIsolationLevel(database, "READ UNCOMMITTED")},
						{Query: "BEGIN"},
						{Query: "SELECT value FROM foo WHERE id = 1", Want: newNullInt(2)},
						{Query: "SELECT value FROM foo WHERE id = 1", Want: newNullInt(20)}, // can't block read skew
						{Query: "ROLLBACK"},
					},
				},
				wantStarts: []string{"0:0", "1:0", "0:1", "1:1", "0:2", "1:2", "0:3", "1:3", "1:4"},
				wantEnds:   []string{"0:0", "1:0", "0:1", "1:1", "0:2", "1:2", "0:3", "1:3", "1:4"},
				skip:       func(d string) bool { return d == "sqlite3" }(database),
			},
			{
				database: database,
				name:     "select w/ transaction after commit, SERIALIZABLE",
				txs: [][]transactonstest.Query{
					{
						{Query: setTransactionIsolationLevel(database, "SERIALIZABLE")},
						{Query: "SELECT 1"},
						{Query: "SELECT 1"},
						{Query: "UPDATE foo SET value = 20 WHERE id = 1"},
					},
					{
						{Query: setTransactionIsolationLevel(database, "SERIALIZABLE")},
						{Query: "BEGIN"},
						{Query: "SELECT value FROM foo WHERE id = 1", Want: newNullInt(2)},
						{Query: "SELECT value FROM foo WHERE id = 1", Want: func(d string) *sql.NullInt64 {
							if d == "postgres" {
								return newNullInt(20) // postgres can't block read skew
							}
							return newNullInt(2)
						}(database)},
						{Query: "ROLLBACK"},
					},
				},
				wantStarts: []string{"0:0", "1:0", "0:1", "1:1", "0:2", "1:2", "0:3", "1:3", "1:4"},
				wantEnds: func(d string) []string {
					if d == "postgres" {
						return []string{"0:0", "1:0", "0:1", "1:1", "0:2", "1:2", "0:3", "1:3", "1:4"}
					}
					return []string{"0:0", "1:0", "0:1", "1:1", "0:2", "1:2", "1:3", "1:4", "0:3"} // update is locked
				}(database),
			},
		}...)
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s/%s", tt.database, tt.name), func(t *testing.T) {
			if tt.skip {
				t.SkipNow()
			}

			ctx := context.Background()
			ctx, cancel := context.WithDeadline(ctx, time.Now().Add(5*time.Second))
			defer cancel()

			db, err := openDB(tt.database)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()

			conn, err := db.Conn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()

			_, err = conn.ExecContext(ctx, "DROP TABLE IF EXISTS foo")
			if err != nil {
				t.Fatal(err)
			}
			_, err = conn.ExecContext(ctx, "CREATE TABLE foo (id INT, value INT)")
			if err != nil {
				t.Fatal(err)
			}
			_, err = conn.ExecContext(ctx, "INSERT INTO foo VALUES (1, 2), (3, 4)")
			if err != nil {
				t.Fatal(err)
			}

			transactonstest.RunTransactionsTest(t, ctx, db, tt.txs, tt.wantStarts, tt.wantEnds)

		})
	}
}
