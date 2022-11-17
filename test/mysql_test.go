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

const (
	MYSQL    = "mysql"
	POSTGRES = "postgres"
	SQLITE   = "sqlite"

	NO_TRANSACTION   = "NO TRANSACTION"
	READ_UNCOMMITTED = "READ UNCOMMITTED"
	READ_COMMITTED   = "READ COMMITTED"
	REPEATABLE_READ  = "REPEATABLE READ"
	SERIALIZABLE     = "SERIALIZABLE"
)

var databases = []string{MYSQL, POSTGRES, SQLITE}
var levels = []string{NO_TRANSACTION, READ_UNCOMMITTED, READ_COMMITTED, REPEATABLE_READ, SERIALIZABLE}
var levelInt = map[string]int{
	NO_TRANSACTION:   0,
	READ_UNCOMMITTED: 1,
	READ_COMMITTED:   2,
	REPEATABLE_READ:  3,
	SERIALIZABLE:     4,
}

func openDB(database string) (*sql.DB, error) {
	switch database {
	case MYSQL:
		return sql.Open("mysql", "root@/test?multiStatements=true")
	case POSTGRES:
		return sql.Open("pgx", "postgres://postgres:postgres@127.0.0.1:5432/postgres")
	case SQLITE:
		// return sql.Open("sqlite3", "file::memory:?cache=shared&_busy_timeout=5000")
		// return sql.Open("sqlite3", "file::memory:?cache=shared")
		return sql.Open("sqlite3", "sqlite3.db")
	default:
		panic(fmt.Errorf("unknown database: %s", database))
	}
}

func startTransaction(database string, level string) string {
	if level == NO_TRANSACTION {
		return "SELECT 1"
	}

	switch database {
	case MYSQL:
		// SET TRANSACTIONは次のトランザクションの分離レベルを変更
		// BEGINでは指定できない
		return fmt.Sprintf("SET TRANSACTION ISOLATION LEVEL %s; BEGIN", level)
		// BEGINで指定できる
		// SET TRANSACTIONは現在のトランザクションの分離レベルを変更
	case POSTGRES:
		return fmt.Sprintf("BEGIN TRANSACTION ISOLATION LEVEL %s", level)
	case SQLITE:
		if level == "READ UNCOMMITTED" {
			return "PRAGMA read_uncommitted = true; BEGIN"
		}

		return "BEGIN"
	default:
		panic(fmt.Errorf("unknown database: %s", database))
	}
}

func commit(level string) string {
	if level == NO_TRANSACTION {
		return "SELECT 1"
	}

	return "COMMIT"
}

func rollback(level string) string {
	if level == NO_TRANSACTION {
		return "SELECT 1"
	}

	return "ROLLBACK"
}

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

type query struct {
	query   string
	want    *sql.NullInt64
	wantOK  *sql.NullInt64
	wantNG  *sql.NullInt64
	wantErr map[string]string
}

type spec struct {
	name       string
	txs        func(database string, level string) [][]query
	threshold  map[string]string
	wantStarts map[string][]string
	wantEnds   map[string][]string
	skip       func(database string, level string) bool
}

var specs = []spec{
	//
	// dirty write
	//
	{
		name: "dirty write",
		txs: func(database string, level string) [][]query {
			return [][]query{
				{
					{query: startTransaction(database, level)},
					{query: "UPDATE foo SET value = 20 WHERE id = 1"},
					{query: "SELECT value FROM foo WHERE id = 1", wantOK: newNullInt(20), wantNG: newNullInt(200)},
					{query: rollback(level)},
				},
				{
					{query: startTransaction(database, level)},
					{query: "UPDATE foo SET value = 200 WHERE id = 1"},
					{query: rollback(level)},
				},
			}
		},
		threshold: map[string]string{"*": READ_UNCOMMITTED},
		wantStarts: map[string][]string{
			NO_TRANSACTION: genSeq(4, 3),
			"*":            {"a:0", "b:0", "a:1", "b:1", "a:2", "a:3", "b:2"},
		},
		wantEnds: map[string][]string{
			NO_TRANSACTION: genSeq(4, 3),
			"*":            {"a:0", "b:0", "a:1", "a:2", "a:3", "b:1", "b:2"},
		},
	},

	//
	// dirty read
	//
	{
		name: "dirty read",
		txs: func(database string, level string) [][]query {
			return [][]query{
				{
					{query: startTransaction(database, level)},
					{query: "UPDATE foo SET value = 20 WHERE id = 1"},
					{query: rollback(level)},
				},
				{
					{query: startTransaction(database, level)},
					{query: "SELECT value FROM foo WHERE id = 1", wantOK: newNullInt(2), wantNG: newNullInt(20)},
					{query: rollback(level)},
				},
			}
		},
		threshold: map[string]string{
			"*":      READ_COMMITTED,
			POSTGRES: READ_UNCOMMITTED,
		},
		wantStarts: map[string][]string{"*": genSeq(3, 3)},
		wantEnds: map[string][]string{
			NO_TRANSACTION: genSeq(3, 3),
			// 変な挙動だったのがなんか急に起こらなくなった
			// SQLITE:                     {"a:0", "b:0", "a:1", "b:1", "b:2", "a:2"}, // tx0:COMMIT will be locked ?
			MYSQL + ":" + SERIALIZABLE: {"a:0", "b:0", "a:1", "a:2", "b:1", "b:2"}, // SELECT is locked
			"*":                        genSeq(3, 3),
		},
	},

	//
	// read skew, (fuzzy read?), Inconsistent Read Anomaly
	// (read skew, fuzzy read の例だと1回目に読み込んでいるのは2回目(fuzzy)とは別のrow？
	// https://qiita.com/kumagi/items/5ef5e404546736ebac49#read-skew-anomaly
	// > 複数の値の間で不一貫な状況を読んでしまう事。
	//
	{
		name: "fuzzy read",
		txs: func(database string, level string) [][]query {
			return [][]query{
				{
					{query: "SELECT 1"},
					{query: "SELECT 1"},
					{query: "UPDATE foo SET value = 20 WHERE id = 1"},
				},
				{
					{query: startTransaction(database, level)},
					{query: "SELECT value FROM foo WHERE id = 1", want: newNullInt(2)},
					{query: "SELECT value FROM foo WHERE id = 1", wantOK: newNullInt(2), wantNG: newNullInt(20)},
					{query: rollback(level)},
				},
			}
		},
		threshold:  map[string]string{"*": REPEATABLE_READ},
		wantStarts: map[string][]string{"*": genSeq(3, 4)},
		wantEnds: map[string][]string{
			MYSQL + ":" + SERIALIZABLE:  {"a:0", "b:0", "a:1", "b:1", "b:2", "b:3", "a:2"}, // UPDATE is locked
			SQLITE + ":" + SERIALIZABLE: {"a:0", "b:0", "a:1", "b:1", "b:2", "b:3", "a:2"}, // UPDATE is locked
			"*":                         genSeq(3, 4),
		},
	},

	//
	// phantom read
	//
	{
		name: "phantom read",
		txs: func(database string, level string) [][]query {
			return [][]query{
				{
					{query: "SELECT 1"},
					{query: "SELECT 1"},
					{query: "INSERT INTO foo VALUES (1, 20)"},
				},
				{
					{query: startTransaction(database, level)},
					{query: "SELECT count(*) FROM foo WHERE id = 1", want: newNullInt(1)},
					{query: "SELECT count(*) FROM foo WHERE id = 1", wantOK: newNullInt(1), wantNG: newNullInt(2)},
					{query: rollback(level)},
				},
			}
		},
		threshold:  map[string]string{"*": REPEATABLE_READ},
		wantStarts: map[string][]string{"*": genSeq(3, 4)},
		wantEnds: map[string][]string{
			MYSQL + ":" + SERIALIZABLE:  {"a:0", "b:0", "a:1", "b:1", "b:2", "b:3", "a:2"}, // INSERT is locked
			SQLITE + ":" + SERIALIZABLE: {"a:0", "b:0", "a:1", "b:1", "b:2", "b:3", "a:2"}, // INSERT is locked
			"*":                         genSeq(3, 4),
		},
	},

	//
	// lost update
	//
	{
		name: "lost update",
		txs: func(database string, level string) [][]query {
			return [][]query{
				{
					{query: startTransaction(database, level)},
					{query: "SELECT value FROM foo WHERE id = 1", want: newNullInt(2)},
					{query: "UPDATE foo SET value = 20 WHERE id = 1"},
					{query: commit(level)},
					{query: "SELECT value FROM foo WHERE id = 1", wantOK: newNullInt(20), wantNG: newNullInt(200)},
				},
				{
					{query: startTransaction(database, level)},
					{query: "SELECT value FROM foo WHERE id = 1", want: newNullInt(2)},
					{query: "UPDATE foo SET value = 200 WHERE id = 1", wantErr: map[string]string{
						MYSQL + ":" + SERIALIZABLE:       "Error 1213: Deadlock found when trying to get lock; try restarting transaction",
						POSTGRES + ":" + REPEATABLE_READ: "ERROR: could not serialize access due to concurrent update (SQLSTATE 40001)",
						POSTGRES + ":" + SERIALIZABLE:    "ERROR: could not serialize access due to concurrent update (SQLSTATE 40001)",
					}},
					{query: commit(level)},
					{query: "SELECT value FROM foo WHERE id = 1", want: newNullInt(200)},
				},
			}
		},
		threshold: map[string]string{
			POSTGRES: REPEATABLE_READ,
			"*":      SERIALIZABLE,
		},
		wantStarts: map[string][]string{
			SERIALIZABLE:                     genSeq(5, 3),
			POSTGRES + ":" + REPEATABLE_READ: genSeq(5, 3),
			"*":                              genSeq(5, 5),
		},
		wantEnds: map[string][]string{
			NO_TRANSACTION:                   genSeq(5, 5),
			SERIALIZABLE:                     {"a:0", "b:0", "a:1", "b:1", "b:2", "a:2", "a:3", "a:4"},
			POSTGRES + ":" + REPEATABLE_READ: {"a:0", "b:0", "a:1", "b:1", "a:2", "a:3", "b:2", "a:4"},
			POSTGRES + ":" + SERIALIZABLE:    {"a:0", "b:0", "a:1", "b:1", "a:2", "a:3", "b:2", "a:4"},               // same as POSTGRES:REPEATABLE_READ
			"*":                              {"a:0", "b:0", "a:1", "b:1", "a:2", "a:3", "b:2", "a:4", "b:3", "b:4"}, // 1:UPDATE is locked
		},
		skip: func(d string, l string) bool { return d == SQLITE && l == SERIALIZABLE }, // "database is locked" won't finish transaction ?
	},

	//
	// write skew
	//
	{
		name: "write skew",
		txs: func(database string, level string) [][]query {
			return [][]query{
				{
					{query: startTransaction(database, level)},
					{query: "SELECT value FROM foo WHERE id = 1", want: newNullInt(2)},  // get X
					{query: "UPDATE foo SET value = 20 WHERE id = 3"},                   // update Y to X*10
					{query: "SELECT value FROM foo WHERE id = 3", want: newNullInt(20)}, // got X*10
					{query: commit(level)},
				},
				{
					{query: startTransaction(database, level)},
					{query: "SELECT value FROM foo WHERE id = 3", want: newNullInt(4)}, // get Y
					// update X to Y*10
					{query: "UPDATE foo SET value = 40 WHERE id = 1", wantErr: map[string]string{
						MYSQL + ":" + SERIALIZABLE: "Error 1213: Deadlock found when trying to get lock; try restarting transaction",
					}},
					{query: "SELECT value FROM foo WHERE id = 1", wantNG: newNullInt(40)}, // write skew: now X=40, Y=20, so not Y = X*10 nor X != Y*10
					{query: commit(level), wantErr: map[string]string{
						POSTGRES + ":" + SERIALIZABLE: "ERROR: could not serialize access due to read/write dependencies among transactions (SQLSTATE 40001)",
					}},
				},
			}
		},
		threshold: map[string]string{"*": SERIALIZABLE},
		wantStarts: map[string][]string{
			MYSQL + ":" + REPEATABLE_READ: {"a:0", "b:0", "a:1", "b:1", "a:2", "b:2", "a:3", "a:4", "b:3", "b:4"}, // 1:update is locked
			MYSQL + ":" + SERIALIZABLE:    genSeq(5, 3),
			"*":                           genSeq(5, 5),
		},
		wantEnds: map[string][]string{
			MYSQL + ":" + REPEATABLE_READ: {"a:0", "b:0", "a:1", "b:1", "a:2", "a:3", "a:4", "b:2", "b:3", "b:4"}, // 1:update is locked
			MYSQL + ":" + SERIALIZABLE:    {"a:0", "b:0", "a:1", "b:1", "b:2", "a:2", "a:3", "a:4"},               // query 0:2 is locked, query1:2 crashes
			"*":                           genSeq(5, 5),
		},
		skip: func(d string, l string) bool { return d == SQLITE && l == SERIALIZABLE }, // "database is locked" won't finish transaction ?
	},
}

func Test(t *testing.T) {
	type test struct {
		database   string
		level      string
		name       string
		txs        [][]query
		threshold  map[string]string
		wantStarts map[string][]string
		wantEnds   map[string][]string
		skip       bool
	}
	tests := make([]test, 0)

	for _, database := range databases {
		for _, level := range levels {
			for _, spec := range specs {
				skip := false
				if spec.skip != nil {
					skip = spec.skip(database, level)
				}

				tests = append(tests, test{
					database:   database,
					level:      level,
					name:       spec.name,
					txs:        spec.txs(database, level),
					threshold:  spec.threshold,
					wantStarts: spec.wantStarts,
					wantEnds:   spec.wantEnds,
					skip:       skip,
				})
			}
		}
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s/%s/%s", tt.database, tt.level, tt.name), func(t *testing.T) {
			if tt.skip {
				t.SkipNow()
			}
			if tt.database == SQLITE && (tt.level != NO_TRANSACTION && tt.level != SERIALIZABLE) {
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

			threshold := tt.threshold["*"]
			if v, ok := tt.threshold[tt.database]; ok {
				threshold = v
			}
			ok := levelInt[tt.level] >= levelInt[threshold]

			txs := make([][]transactonstest.Query, len(tt.txs))
			for i, queries := range tt.txs {
				txs[i] = make([]transactonstest.Query, len(tt.txs[i]))
				for j, q := range queries {
					want := q.want
					if want == nil {
						if ok {
							want = q.wantOK
						} else {
							want = q.wantNG
						}
					}

					txs[i][j] = transactonstest.Query{
						Query:   q.query,
						Want:    want,
						WantErr: q.wantErr[tt.database+":"+tt.level],
					}
				}
			}

			wantStarts := tt.wantStarts["*"]
			if v, ok := tt.wantStarts[tt.database+":"+tt.level]; ok {
				wantStarts = v
			} else if v, ok := tt.wantStarts[tt.level]; ok {
				wantStarts = v
			} else if v, ok := tt.wantStarts[tt.database]; ok {
				wantStarts = v
			}

			wantEnds := tt.wantEnds["*"]
			if v, ok := tt.wantEnds[tt.database+":"+tt.level]; ok {
				wantEnds = v
			} else if v, ok := tt.wantEnds[tt.level]; ok {
				wantEnds = v
			} else if v, ok := tt.wantEnds[tt.database]; ok {
				wantEnds = v
			}

			transactonstest.RunTransactionsTest(t, ctx, db, txs, wantStarts, wantEnds)

		})
	}
}
