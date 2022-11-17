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

// 	wantStarts := []string{"a:0", "b:0", "a:1", "b:1"}
// 	wantEnds := []string{"a:0", "a:1", "b:0", "b:1"}

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

// 	wantStarts := []string{"a:0", "b:0", "a:1", "b:1"}
// 	wantEnds := []string{"a:0", "a:1", "b:0", "b:1"}

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
		return sql.Open("mysql", "root@/test?multiStatements=true")
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

func startTransaction(database string, level string) string {
	switch database {
	case "mysql":
		// SET TRANSACTIONは次のトランザクションの分離レベルを変更
		// BEGINでは指定できない
		return fmt.Sprintf("SET TRANSACTION ISOLATION LEVEL %s; BEGIN", level)
		// BEGINで指定できる
		// SET TRANSACTIONは現在のトランザクションの分離レベルを変更
	case "postgres":
		return fmt.Sprintf("BEGIN TRANSACTION ISOLATION LEVEL %s", level)
	case "sqlite3":
		if level == "READ UNCOMMITTED" {
			return "PRAGMA read_uncommitted = true; BEGIN"
		}

		return "BEGIN"
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

func genSeq(m, n int) []string {
	seq := make([]string, 0, m+n)

	i := 0

	for {
		if i < m {
			seq = append(seq, fmt.Sprintf("%s:%d", "a", i))
		}
		if i < n {
			seq = append(seq, fmt.Sprintf("%s:%d", "b", i))
		}
		if i >= m && i >= n {
			break
		}

		i++
	}

	return seq
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
						{Query: "SELECT value FROM foo WHERE id = 1", Want: newNullInt(200)}, // dirty write
					},
					{
						{Query: "UPDATE foo SET value = 200 WHERE id = 1"},
					},
				},
				wantStarts: genSeq(2, 1),
				wantEnds:   genSeq(2, 1),
			},
			{
				database: database,
				name:     "lock w/ transaction",
				txs: [][]transactonstest.Query{
					{
						{Query: startTransaction(database, "READ UNCOMMITTED")},
						{Query: "UPDATE foo SET value = 20 WHERE id = 1"},
						{Query: "SELECT value FROM foo WHERE id = 1", Want: newNullInt(20)},
						{Query: "ROLLBACK"},
					},
					{
						{Query: startTransaction(database, "READ UNCOMMITTED")},
						{Query: "UPDATE foo SET value = 200 WHERE id = 1"},
						{Query: "ROLLBACK"},
					},
				},
				wantStarts: []string{"a:0", "b:0", "a:1", "b:1", "a:2", "a:3", "b:2"},
				wantEnds:   []string{"a:0", "b:0", "a:1", "a:2", "a:3", "b:1", "b:2"},
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
				wantStarts: []string{"a:0", "b:0"},
				// wantEnds:   []string{"a:0", "b:0"}, // mysql fails, maybe too fast to select
			},
			{
				database: database,
				name:     "select before commit READ UNCOMMITTED",
				txs: [][]transactonstest.Query{
					{
						{Query: startTransaction(database, "READ UNCOMMITTED")},
						{Query: "UPDATE foo SET value = 20 WHERE id = 1"},
						{Query: "COMMIT"},
					},
					{
						{Query: startTransaction(database, "READ UNCOMMITTED")},
						{Query: "SELECT value FROM foo WHERE id = 1", Want: func(d string) *sql.NullInt64 {
							if d == "postgres" {
								return newNullInt(2) // postgres's READ UNCOMMITTED is READ COMMITTED, so no dirty read
							}
							if d == "sqlite3" {
								return newNullInt(2) // no dirty read ?
							}
							return newNullInt(20) // dirty read
						}(database)},
						{Query: "ROLLBACK"},
					},
				},
				wantStarts: genSeq(3, 3),
				wantEnds: func(d string) []string {
					if d == "sqlite3" {
						// tx0:COMMIT will be locked ?
						return []string{"a:0", "b:0", "a:1", "b:1", "b:2", "a:2"}
					}
					return genSeq(3, 3)
				}(database),
			},
			{
				database: database,
				name:     "select before commit READ COMMITTED",
				txs: [][]transactonstest.Query{
					{
						{Query: startTransaction(database, "READ COMMITTED")},
						{Query: "UPDATE foo SET value = 20 WHERE id = 1"},
						{Query: "COMMIT"},
					},
					{
						{Query: startTransaction(database, "READ COMMITTED")},
						{Query: "SELECT value FROM foo WHERE id = 1", Want: newNullInt(2)}, // no dirty read
						{Query: "ROLLBACK"},
					},
				},
				wantStarts: genSeq(3, 3),
				wantEnds:   genSeq(3, 3),
				skip:       func(d string) bool { return d == "sqlite3" }(database),
			},
			{
				database: database,
				name:     "select before commit REPEATABLE READ",
				txs: [][]transactonstest.Query{
					{
						{Query: startTransaction(database, "REPEATABLE READ")},
						{Query: "UPDATE foo SET value = 20 WHERE id = 1"},
						{Query: "COMMIT"},
					},
					{
						{Query: startTransaction(database, "REPEATABLE READ")},
						{Query: "SELECT value FROM foo WHERE id = 1", Want: newNullInt(2)},
						{Query: "ROLLBACK"},
					},
				},
				wantStarts: genSeq(3, 3),
				wantEnds:   genSeq(3, 3),
				skip:       func(d string) bool { return d == "sqlite3" }(database),
			},
			{
				database: database,
				name:     "select before commit SERIALIZABLE",
				txs: [][]transactonstest.Query{
					{
						{Query: startTransaction(database, "SERIALIZABLE")},
						{Query: "UPDATE foo SET value = 20 WHERE id = 1"},
						{Query: "COMMIT"},
					},
					{
						{Query: startTransaction(database, "SERIALIZABLE")},
						{Query: "SELECT value FROM foo WHERE id = 1", Want: func(d string) *sql.NullInt64 {
							if d == "mysql" {
								return newNullInt(20) // locked and read committed value
							}

							return newNullInt(2)
						}(database)},
						{Query: "ROLLBACK"},
					},
				},
				wantStarts: genSeq(3, 3),
				wantEnds: func(d string) []string {
					if d == "sqlite3" {
						// tx0:COMMIT will be locked ?
						return []string{"a:0", "b:0", "a:1", "b:1", "b:2", "a:2"}
					}
					if d == "mysql" {
						// tx1: SELECT will be locked
						return []string{"a:0", "b:0", "a:1", "a:2", "b:1", "b:2"}
					}
					return genSeq(3, 3)
				}(database),
			},

			//
			// read skew, (fuzzy read?), Inconsistent Read Anomaly
			// (read skew, fuzzy read の例だと1回目に読み込んでいるのは2回目(fuzzy)とは別のrow？
			// https://qiita.com/kumagi/items/5ef5e404546736ebac49#read-skew-anomaly
			// > 複数の値の間で不一貫な状況を読んでしまう事。
			//
			{
				database: database,
				name:     "select after commit READ COMMITTED",
				txs: [][]transactonstest.Query{
					{
						{Query: "SELECT 1"},
						{Query: "SELECT 1"},
						{Query: "UPDATE foo SET value = 20 WHERE id = 1"},
					},
					{
						{Query: startTransaction(database, "READ COMMITTED")},
						{Query: "SELECT value FROM foo WHERE id = 1", Want: newNullInt(2)},
						{Query: "SELECT value FROM foo WHERE id = 1", Want: newNullInt(20)}, // can't block read skew
						{Query: "ROLLBACK"},
					},
				},
				wantStarts: genSeq(3, 4),
				wantEnds:   genSeq(3, 4),
				skip:       func(d string) bool { return d == "sqlite3" }(database),
			},
			{
				database: database,
				name:     "select after commit REPEATABLE READ",
				txs: [][]transactonstest.Query{
					{
						{Query: "SELECT 1"},
						{Query: "SELECT 1"},
						{Query: "UPDATE foo SET value = 20 WHERE id = 1"},
					},
					{
						{Query: startTransaction(database, "REPEATABLE READ")},
						{Query: "SELECT value FROM foo WHERE id = 1", Want: newNullInt(2)},
						{Query: "SELECT value FROM foo WHERE id = 1", Want: newNullInt(2)},
						{Query: "ROLLBACK"},
					},
				},
				wantStarts: genSeq(3, 4),
				wantEnds:   genSeq(3, 4), // no lock
				skip:       func(d string) bool { return d == "sqlite3" }(database),
			},
			{
				database: database,
				name:     "select after commit SERIALIZABLE",
				txs: [][]transactonstest.Query{
					{
						{Query: "SELECT 1"},
						{Query: "SELECT 1"},
						{Query: "UPDATE foo SET value = 20 WHERE id = 1"},
					},
					{
						{Query: startTransaction(database, "SERIALIZABLE")},
						{Query: "SELECT value FROM foo WHERE id = 1", Want: newNullInt(2)},
						{Query: "SELECT value FROM foo WHERE id = 1", Want: newNullInt(2)},
						{Query: "ROLLBACK"},
					},
				},
				wantStarts: genSeq(3, 4),
				wantEnds: func(d string) []string {
					if d == "postgres" {
						return genSeq(3, 4) // no lock, same as REPEATABLE READ
					}
					return []string{"a:0", "b:0", "a:1", "b:1", "b:2", "b:3", "a:2"} // update is locked
				}(database),
			},

			//
			// phantom read
			//
			{
				database: database,
				name:     "select after insert READ COMMITTED",
				txs: [][]transactonstest.Query{
					{
						{Query: "SELECT 1"},
						{Query: "SELECT 1"},
						{Query: "INSERT INTO foo VALUES (1, 20)"},
					},
					{
						{Query: startTransaction(database, "READ COMMITTED")},
						{Query: "SELECT count(*) FROM foo WHERE id = 1", Want: newNullInt(1)},
						{Query: "SELECT count(*) FROM foo WHERE id = 1", Want: newNullInt(2)}, // read phantom
						{Query: "ROLLBACK"},
					},
				},
				wantStarts: genSeq(3, 4),
				wantEnds:   genSeq(3, 4), // no lock
				skip:       func(d string) bool { return d == "sqlite3" }(database),
			},
			{
				database: database,
				name:     "select after insert REPEATABLE READ",
				txs: [][]transactonstest.Query{
					{
						{Query: "SELECT 1"},
						{Query: "SELECT 1"},
						{Query: "INSERT INTO foo VALUES (1, 20)"},
					},
					{
						{Query: startTransaction(database, "REPEATABLE READ")},
						{Query: "SELECT count(*) FROM foo WHERE id = 1", Want: newNullInt(1)},
						{Query: "SELECT count(*) FROM foo WHERE id = 1", Want: newNullInt(1)},
						{Query: "ROLLBACK"},
					},
				},
				wantStarts: genSeq(3, 4),
				wantEnds:   genSeq(3, 4), // no lock
				skip:       func(d string) bool { return d == "sqlite3" }(database),
			},
			{
				database: database,
				name:     "select after insert SERIALIZABLE",
				txs: [][]transactonstest.Query{
					{
						{Query: "SELECT 1"},
						{Query: "SELECT 1"},
						{Query: "INSERT INTO foo VALUES (1, 20)"},
					},
					{
						{Query: startTransaction(database, "SERIALIZABLE")},
						{Query: "SELECT count(*) FROM foo WHERE id = 1", Want: newNullInt(1)},
						{Query: "SELECT count(*) FROM foo WHERE id = 1", Want: newNullInt(1)},
						{Query: "ROLLBACK"},
					},
				},
				wantStarts: genSeq(3, 4),
				wantEnds: func(d string) []string {
					if d == "postgres" {
						return genSeq(3, 4) // no lock
					}
					return []string{"a:0", "b:0", "a:1", "b:1", "b:2", "b:3", "a:2"} // INSERT is locked
				}(database),
			},

			//
			// lost update
			//
			{
				database: database,
				name:     "update after update READ COMMITTED",
				txs: [][]transactonstest.Query{
					{
						{Query: startTransaction(database, "READ COMMITTED")},
						{Query: "SELECT value FROM foo WHERE id = 1", Want: newNullInt(2)},
						{Query: "UPDATE foo SET value = 20 WHERE id = 1"},
						{Query: "COMMIT"},
						{Query: "SELECT value FROM foo WHERE id = 1", Want: newNullInt(200)}, // lost update
					},
					{
						{Query: startTransaction(database, "READ COMMITTED")},
						{Query: "SELECT value FROM foo WHERE id = 1", Want: newNullInt(2)},
						{Query: "UPDATE foo SET value = 200 WHERE id = 1"},
						{Query: "COMMIT"},
						{Query: "SELECT value FROM foo WHERE id = 1", Want: newNullInt(200)},
					},
				},
				wantStarts: genSeq(5, 5),
				wantEnds:   []string{"a:0", "b:0", "a:1", "b:1", "a:2", "a:3", "b:2", "a:4", "b:3", "b:4"}, // 1:UPDATE is locked
				skip:       func(d string) bool { return d == "sqlite3" }(database),
			},
			{
				database: database,
				name:     "update after update REPEATABLE READ",
				txs: [][]transactonstest.Query{
					{
						{Query: startTransaction(database, "REPEATABLE READ")},
						{Query: "SELECT value FROM foo WHERE id = 1", Want: newNullInt(2)},
						{Query: "UPDATE foo SET value = 20 WHERE id = 1"},
						{Query: "COMMIT"},
						{Query: "SELECT value FROM foo WHERE id = 1", Want: func(d string) *sql.NullInt64 {
							if d == "postgres" {
								return newNullInt(20) // no lost update
							}
							return newNullInt(200)
						}(database)},
					},
					{
						{Query: startTransaction(database, "REPEATABLE READ")},
						{Query: "SELECT value FROM foo WHERE id = 1", Want: newNullInt(2)},
						{Query: "UPDATE foo SET value = 200 WHERE id = 1", WantErr: func(d string) error {
							if d == "postgres" {
								return fmt.Errorf("ERROR: could not serialize access due to concurrent update (SQLSTATE 40001)")
							}
							return nil
						}(database)},
						{Query: "COMMIT"},
						{Query: "SELECT value FROM foo WHERE id = 1", Want: newNullInt(200)},
					},
				},
				wantStarts: func(d string) []string {
					if d == "postgres" {
						return genSeq(5, 3) // 3th query crashes
					}
					return genSeq(5, 5)
				}(database),
				wantEnds: func(d string) []string {
					if d == "postgres" {
						return []string{"a:0", "b:0", "a:1", "b:1", "a:2", "a:3", "b:2", "a:4"} // 1:UPDATE is locked, and 2nd UPDATE crashes
					}
					return []string{"a:0", "b:0", "a:1", "b:1", "a:2", "a:3", "b:2", "a:4", "b:3", "b:4"} // 1:UPDATE is locked
				}(database),
				skip: func(d string) bool {
					return d == "sqlite3"
				}(database),
			},
			{
				database: database,
				name:     "update after update SERIALIZABLE",
				txs: [][]transactonstest.Query{
					{
						{Query: startTransaction(database, "SERIALIZABLE")},
						{Query: "SELECT value FROM foo WHERE id = 1", Want: newNullInt(2)},
						{Query: "UPDATE foo SET value = 20 WHERE id = 1"},
						{Query: "COMMIT"},
						{Query: "SELECT value FROM foo WHERE id = 1", Want: newNullInt(20)},
					},
					{
						{Query: startTransaction(database, "SERIALIZABLE")},
						{Query: "SELECT value FROM foo WHERE id = 1", Want: newNullInt(2)},
						{Query: "UPDATE foo SET value = 200 WHERE id = 1", WantErr: func(d string) error {
							if d == "mysql" {
								return fmt.Errorf("Error 1213: Deadlock found when trying to get lock; try restarting transaction")
							}
							if d == "postgres" {
								return fmt.Errorf("ERROR: could not serialize access due to concurrent update (SQLSTATE 40001)")
							}
							return nil
						}(database)},
						{Query: "COMMIT"},
						{Query: "SELECT value FROM foo WHERE id = 1", Want: newNullInt(200)},
					},
				},
				wantStarts: []string{"a:0", "b:0", "a:1", "b:1", "a:2", "b:2", "a:3", "a:4"},
				wantEnds: func(d string) []string {
					if d == "postgres" {
						return []string{"a:0", "b:0", "a:1", "b:1", "a:2", "a:3", "b:2", "a:4"} // 1:UPDATE locked and then crashes
					}
					return []string{"a:0", "b:0", "a:1", "b:1", "b:2", "a:2", "a:3", "a:4"}
				}(database),
				skip: func(d string) bool {
					return d == "sqlite3"
				}(database),
			},

			//
			// write skew
			//
			{
				database: database,
				name:     "select and write each other REPEATABLE READ",
				txs: [][]transactonstest.Query{
					{
						{Query: startTransaction(database, "REPEATABLE READ")},
						{Query: "SELECT value FROM foo WHERE id = 1", Want: newNullInt(2)}, // get X
						{Query: "UPDATE foo SET value = 20 WHERE id = 3"},                  // update Y to X*10
						{Query: "SELECT value FROM foo WHERE id = 3", Want: newNullInt(20)},
						{Query: "COMMIT"},
					},
					{
						{Query: startTransaction(database, "REPEATABLE READ")},
						{Query: "SELECT value FROM foo WHERE id = 3", Want: newNullInt(4)},  // get Y
						{Query: "UPDATE foo SET value = 40 WHERE id = 1"},                   // update X to Y*10
						{Query: "SELECT value FROM foo WHERE id = 1", Want: newNullInt(40)}, // write skew: now X=40, Y=20, so not Y = X*10 nor X != Y*10
						{Query: "COMMIT"},
					},
				},
				wantStarts: func(d string) []string {
					if d == "mysql" {
						return []string{"a:0", "b:0", "a:1", "b:1", "a:2", "b:2", "a:3", "a:4", "b:3", "b:4"} // 1:update is locked
					}
					return genSeq(5, 5)
				}(database),
				wantEnds: func(d string) []string {
					if d == "mysql" {
						return []string{"a:0", "b:0", "a:1", "b:1", "a:2", "a:3", "a:4", "b:2", "b:3", "b:4"} // 1:update is locked
					}
					return genSeq(5, 5)
				}(database),
				skip: func(d string) bool { return d == "sqlite3" }(database),
			},
			{
				database: database,
				name:     "select and write each other SERIALIZABLE",
				txs: [][]transactonstest.Query{
					{
						{Query: startTransaction(database, "SERIALIZABLE")},
						{Query: "SELECT value FROM foo WHERE id = 1", Want: newNullInt(2)},
						{Query: "UPDATE foo SET value = 20 WHERE id = 3"},
						{Query: "SELECT value FROM foo WHERE id = 3", Want: newNullInt(20)},
						{Query: "COMMIT"},
					},
					{
						{Query: startTransaction(database, "SERIALIZABLE")},
						{Query: "SELECT value FROM foo WHERE id = 3", Want: newNullInt(4)},
						{Query: "UPDATE foo SET value = 40 WHERE id = 1", WantErr: func(d string) error {
							if d == "mysql" {
								return fmt.Errorf("Error 1213: Deadlock found when trying to get lock; try restarting transaction")
							}
							return nil
						}(database)},
						{Query: "SELECT value FROM foo WHERE id = 1", Want: newNullInt(40)},
						{Query: "COMMIT", WantErr: fmt.Errorf("ERROR: could not serialize access due to read/write dependencies among transactions (SQLSTATE 40001)")},
					},
				},
				wantStarts: func(d string) []string {
					if d == "mysql" {
						return genSeq(5, 3) // query1:2 crashes
					}
					return genSeq(5, 5)
				}(database),
				wantEnds: func(d string) []string {
					if d == "mysql" {
						return []string{"a:0", "b:0", "a:1", "b:1", "b:2", "a:2", "a:3", "a:4"} // query 0:2 is locked, query1:2 crashes
					}
					return genSeq(5, 5)
				}(database),
				skip: func(d string) bool {
					return d == "sqlite3"
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
