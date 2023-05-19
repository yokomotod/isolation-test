package test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/ibmdb/go_ibm_db"
	_ "github.com/jackc/pgx/v5"
	_ "github.com/mattn/go-sqlite3"
	_ "github.com/microsoft/go-mssqldb"
	go_ora "github.com/sijms/go-ora/v2"
	"github.com/yokomotod/isolation-test/pkg/transactonstest"
	"golang.org/x/exp/slices"
)

const (
	MYSQL     = "mysql"
	POSTGRES  = "postgres"
	SQLSERVER = "sqlserver"
	ORACLE    = "oracle"
	DB2       = "db2"
	SQLITE    = "sqlite"

	NO_TRANSACTION          = "NO TRANSACTION"
	READ_UNCOMMITTED        = "READ UNCOMMITTED"
	READ_COMMITTED          = "READ COMMITTED"
	READ_COMMITTED_SNAPSHOT = "READ COMMITTED SNAPSHOT"
	CURSOR_STABILITY        = "CURSOR STABILITY"
	READ_STABILITY          = "RS"
	REPEATABLE_READ         = "REPEATABLE READ"
	REPEATABLE_READ_LOCK    = "REPEATABLE READ LOCK"
	SNAPSHOT                = "SNAPSHOT"
	SERIALIZABLE            = "SERIALIZABLE"
	NEVER                   = "NEVER"
)

var databases = []string{MYSQL, POSTGRES, SQLSERVER, ORACLE, DB2}
var dbLevels = map[string][]string{
	SQLSERVER: {
		NO_TRANSACTION,
		READ_UNCOMMITTED,
		READ_COMMITTED,
		READ_COMMITTED_SNAPSHOT,
		REPEATABLE_READ,
		SNAPSHOT,
		SERIALIZABLE,
	},
	ORACLE: {
		NO_TRANSACTION,
		READ_COMMITTED,
		SERIALIZABLE,
	},
	DB2: {
		NO_TRANSACTION,
		READ_UNCOMMITTED,
		READ_COMMITTED,
		CURSOR_STABILITY,
		READ_STABILITY,
		REPEATABLE_READ,
		SERIALIZABLE,
	},
	"*": {
		NO_TRANSACTION,
		READ_UNCOMMITTED,
		READ_COMMITTED,
		REPEATABLE_READ,
		// REPEATABLE_READ_LOCK,
		SERIALIZABLE,
	},
}
var levelInt = map[string]int{
	NO_TRANSACTION:          0,
	READ_UNCOMMITTED:        1,
	READ_COMMITTED:          2,
	READ_COMMITTED_SNAPSHOT: 3,
	CURSOR_STABILITY:        4,
	READ_STABILITY:          5,
	REPEATABLE_READ:         6,
	REPEATABLE_READ_LOCK:    6,
	SNAPSHOT:                6,
	SERIALIZABLE:            7,
	NEVER:                   9,
}

func openDB(database, level string) (*sql.DB, error) {
	switch database {
	case MYSQL:
		return sql.Open("mysql", "root@/test?multiStatements=true")
	case POSTGRES:
		return sql.Open("pgx", "postgres://postgres:postgres@127.0.0.1:5432/postgres")
	case SQLSERVER:
		if level == READ_COMMITTED_SNAPSHOT || level == SNAPSHOT {
			// `ALTER DATABASE test2 SET ALLOW_SNAPSHOT_ISOLATION ON`
			// `ALTER DATABASE test2 SET READ_COMMITTED_SNAPSHOT ON``
			return sql.Open("sqlserver", "server=127.0.0.1;user id=SA;password=Passw0rd;database=test2;")
		}
		return sql.Open("sqlserver", "server=127.0.0.1;user id=SA;password=Passw0rd;database=test1;")
	case ORACLE:
		url := go_ora.BuildUrl("127.0.0.1", 1521, "XE", "system", "password", nil) // map[string]string{"DBA PRIVILEGE": "SYSDBA"},

		return sql.Open("oracle", url)
	case DB2:
		// $ pushd $(go env GOPATH)/pkg/mod/github.com/ibmdb/go_ibm_db\@v0.4.3/installer
		// $ source setenv.sh
		// $ popd
		if level == CURSOR_STABILITY {
			// `UPDATE DATABASE CONFIGURATION for TESTDB2 USING cur_commit DISABLED`
			return sql.Open("go_ibm_db", "HOSTNAME=127.0.0.1;DATABASE=testdb2;PORT=50000;UID=db2inst1;PWD=password")
		}
		return sql.Open("go_ibm_db", "HOSTNAME=127.0.0.1;DATABASE=testdb;PORT=50000;UID=db2inst1;PWD=password")
	case SQLITE:
		// return sql.Open("sqlite3", "file::memory:?cache=shared&_busy_timeout=5000")
		// return sql.Open("sqlite3", "file::memory:?cache=shared")
		return sql.Open("sqlite3", "sqlite3.db")
	default:
		panic(fmt.Errorf("unknown database: %s", database))
	}
}

func getIsolationLevel(database string, level string) sql.IsolationLevel {
	if level == NO_TRANSACTION {
		return -1
	}

	if database == ORACLE {
		// go-ora only supports default value for isolation
		// https://github.com/sijms/go-ora/blob/v2.7.2/v2/connection.go#L555
		return sql.LevelDefault
	}
	if database == DB2 {
		// go_ibm_db driver does not support non-default isolation level
		return sql.LevelDefault
	}

	switch level {
	case READ_UNCOMMITTED:
		return sql.LevelReadUncommitted
	case READ_COMMITTED:
		fallthrough
	case READ_COMMITTED_SNAPSHOT:
		return sql.LevelReadCommitted
	case REPEATABLE_READ:
		fallthrough
	case REPEATABLE_READ_LOCK:
		return sql.LevelRepeatableRead
	case SNAPSHOT:
		return sql.LevelSnapshot
	case SERIALIZABLE:
		return sql.LevelSerializable
	default:
		panic(fmt.Errorf("unknown level: %s", level))
	}
}

func startTransaction(database string, level string, tx int) string {
	if level == NO_TRANSACTION {
		return ""
	}

	switch database {
	// case MYSQL:
	// 	// SET TRANSACTIONは次のトランザクションの分離レベルを変更
	// 	// BEGINでは指定できない
	// 	return fmt.Sprintf("SET TRANSACTION ISOLATION LEVEL %s; BEGIN", level)
	// case POSTGRES:
	// 	// BEGINで指定できる
	// 	// SET TRANSACTIONは現在のトランザクションの分離レベルを変更
	// 	return fmt.Sprintf("BEGIN TRANSACTION ISOLATION LEVEL %s", level)
	case SQLSERVER:
		if tx == 0 {
			return "BEGIN; SET DEADLOCK_PRIORITY HIGH"
		}
		return "BEGIN"
	case ORACLE:
		return fmt.Sprintf("BEGIN; SET TRANSACTION ISOLATION LEVEL %s", level)
	case DB2:
		return fmt.Sprintf("BEGIN; SET CURRENT ISOLATION TO %s", level)
	// case SQLITE:
	// 	if level == "READ UNCOMMITTED" {
	// 		return "PRAGMA read_uncommitted = true; BEGIN"
	// 	}

	// 	return "BEGIN"
	default:
		// panic(fmt.Errorf("unknown database: %s", database))
		return "BEGIN"
	}
}

func commit(database string, level string) string {
	if level == NO_TRANSACTION {
		return ""
	}

	if database == SQLSERVER {
		return "COMMIT TRANSACTION"
	}

	return "COMMIT"
}

func rollback(database string, level string) string {
	if level == NO_TRANSACTION {
		return ""
	}

	if database == SQLSERVER {
		return "ROLLBACK TRANSACTION"
	}

	return "ROLLBACK"
}

func newNullInts(values ...int64) []sql.NullInt64 {
	res := make([]sql.NullInt64, len(values))
	for i, v := range values {
		res[i] = sql.NullInt64{Int64: v, Valid: true}
	}
	return res
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
	Query   string                     `json:"query"`
	Want    map[string][]sql.NullInt64 `json:"want"`
	WantOK  map[string][]sql.NullInt64 `json:"wantOk"`
	WantNG  map[string][]sql.NullInt64 `json:"wantNg"`
	WantErr map[string]string          `json:"wantErr"`
	compile map[string]bool
}

type spec struct {
	Name         string              `json:"name"`
	Txs          [][]query           `json:"txs"`
	Threshold    map[string]string   `json:"threshold"`
	AdditionalOk map[string][]string `json:"additionalOk"`
	WantStarts   map[string][]string `json:"wantStarts"`
	WantEnds     map[string][]string `json:"wantEnds"`
	Skip         map[string]bool     `json:"skip"`
}

var specs = []spec{
	// {
	// 	Name: "dirty write",
	// 	Txs: [][]query{
	// 		{
	// 			{Query: "BEGIN"},
	// 			{Query: "UPDATE foo SET value = 20 WHERE id = 1"},
	// 			{Query: "SELECT value FROM foo WHERE id = 1",
	// 				WantOK: map[string][]sql.NullInt64{"*": newNullInts(20)},
	// 				WantNG: map[string][]sql.NullInt64{"*": newNullInts(200)},
	// 			},
	// 			{Query: "COMMIT"},
	// 		},
	// 		{
	// 			{Query: "BEGIN"},
	// 			{Query: "UPDATE foo SET value = 200 WHERE id = 1"},
	// 			{Query: "COMMIT"},
	// 		},
	// 	},
	// 	Threshold: map[string]string{"*": READ_UNCOMMITTED},
	// 	WantStarts: map[string][]string{
	// 		NO_TRANSACTION: genSeq(4, 3),
	// 		"*":            {"a:0", "b:0", "a:1", "b:1", "a:2", "a:3", "b:2"},
	// 	},
	// 	WantEnds: map[string][]string{
	// 		NO_TRANSACTION: genSeq(4, 3),
	// 		"*":            {"a:0", "b:0", "a:1", "a:2", "a:3", "b:1", "b:2"},
	// 	},
	// },

	{
		Name: "dirty read",
		Txs: [][]query{
			{
				{Query: "BEGIN"},
				{Query: "UPDATE foo SET value = 20 WHERE id = 1"},
				{Query: "COMMIT"},
			},
			{
				{Query: "BEGIN"},
				{Query: "SELECT value FROM foo WHERE id = 1",
					Want: map[string][]sql.NullInt64{
						NO_TRANSACTION:                    nil,
						READ_UNCOMMITTED:                  nil,
						MYSQL + ":" + SERIALIZABLE:        newNullInts(20),
						SQLSERVER + ":" + READ_COMMITTED:  newNullInts(20),
						SQLSERVER + ":" + REPEATABLE_READ: newNullInts(20),
						SQLSERVER + ":" + SERIALIZABLE:    newNullInts(20),
						DB2 + ":" + CURSOR_STABILITY:      newNullInts(20),
						DB2 + ":" + READ_STABILITY:        newNullInts(20),
						DB2 + ":" + REPEATABLE_READ:       newNullInts(20),
						DB2 + ":" + SERIALIZABLE:          newNullInts(20),
					},
					WantOK: map[string][]sql.NullInt64{"*": newNullInts(2)},
					WantNG: map[string][]sql.NullInt64{"*": newNullInts(20)},
				},
				{Query: "COMMIT"},
			},
		},
		Threshold: map[string]string{
			"*":      READ_COMMITTED,
			POSTGRES: READ_UNCOMMITTED,
		},
		WantStarts: map[string][]string{"*": genSeq(3, 3)},
		WantEnds: map[string][]string{
			NO_TRANSACTION: genSeq(3, 3),
			// 変な挙動だったのがなんか急に起こらなくなった
			// SQLITE:                     {"a:0", "b:0", "a:1", "b:1", "b:2", "a:2"}, // tx0:COMMIT will be locked ?
			REPEATABLE_READ_LOCK:              {"a:0", "b:0", "a:1", "a:2", "b:1", "b:2"}, // SELECT is locked
			MYSQL + ":" + SERIALIZABLE:        {"a:0", "b:0", "a:1", "a:2", "b:1", "b:2"}, // SELECT is locked
			SQLSERVER + ":" + READ_COMMITTED:  {"a:0", "b:0", "a:1", "a:2", "b:1", "b:2"}, // SELECT is locked
			SQLSERVER + ":" + REPEATABLE_READ: {"a:0", "b:0", "a:1", "a:2", "b:1", "b:2"}, // SELECT is locked
			SQLSERVER + ":" + SERIALIZABLE:    {"a:0", "b:0", "a:1", "a:2", "b:1", "b:2"}, // SELECT is locked
			DB2 + ":" + CURSOR_STABILITY:      {"a:0", "b:0", "a:1", "a:2", "b:1", "b:2"}, // SELECT is locked
			DB2 + ":" + READ_STABILITY:        {"a:0", "b:0", "a:1", "a:2", "b:1", "b:2"}, // SELECT is locked
			DB2 + ":" + REPEATABLE_READ:       {"a:0", "b:0", "a:1", "a:2", "b:1", "b:2"}, // SELECT is locked
			DB2 + ":" + SERIALIZABLE:          {"a:0", "b:0", "a:1", "a:2", "b:1", "b:2"}, // SELECT is locked
			"*":                               genSeq(3, 3),
		},
	},

	//
	// read skew, (fuzzy read?), Inconsistent Read Anomaly
	// (read skew, fuzzy read の例だと1回目に読み込んでいるのは2回目(fuzzy)とは別のrow？
	// https://qiita.com/kumagi/items/5ef5e404546736ebac49#read-skew-anomaly
	// > 複数の値の間で不一貫な状況を読んでしまう事。
	//
	{
		Name: "fuzzy read",
		Txs: [][]query{
			{
				{},
				{},
				{Query: "UPDATE foo SET value = 20 WHERE id = 1"},
			},
			{
				{Query: "BEGIN"},
				{Query: "SELECT value FROM foo WHERE id = 1", Want: map[string][]sql.NullInt64{"*": newNullInts(2)}},
				{Query: "SELECT value FROM foo WHERE id = 1", WantOK: map[string][]sql.NullInt64{"*": newNullInts(2)}, WantNG: map[string][]sql.NullInt64{"*": newNullInts(20)}},
				{Query: "COMMIT"},
			},
		},
		Threshold: map[string]string{
			DB2: READ_STABILITY,
			"*": REPEATABLE_READ,
		},
		WantStarts: map[string][]string{"*": genSeq(3, 4)},
		WantEnds: map[string][]string{
			REPEATABLE_READ_LOCK:              {"a:0", "b:0", "a:1", "b:1", "b:2", "b:3", "a:2"}, // UPDATE is locked
			MYSQL + ":" + SERIALIZABLE:        {"a:0", "b:0", "a:1", "b:1", "b:2", "b:3", "a:2"}, // UPDATE is locked
			SQLSERVER + ":" + REPEATABLE_READ: {"a:0", "b:0", "a:1", "b:1", "b:2", "b:3", "a:2"}, // UPDATE is locked
			SQLSERVER + ":" + SERIALIZABLE:    {"a:0", "b:0", "a:1", "b:1", "b:2", "b:3", "a:2"}, // UPDATE is locked
			DB2 + ":" + READ_STABILITY:        {"a:0", "b:0", "a:1", "b:1", "b:2", "b:3", "a:2"}, // UPDATE is locked
			DB2 + ":" + REPEATABLE_READ:       {"a:0", "b:0", "a:1", "b:1", "b:2", "b:3", "a:2"}, // UPDATE is locked
			DB2 + ":" + SERIALIZABLE:          {"a:0", "b:0", "a:1", "b:1", "b:2", "b:3", "a:2"}, // UPDATE is locked
			SQLITE + ":" + SERIALIZABLE:       {"a:0", "b:0", "a:1", "b:1", "b:2", "b:3", "a:2"}, // UPDATE is locked
			"*":                               genSeq(3, 4),
		},
	},

	{
		Name: "phantom read",
		Txs: [][]query{
			{
				// {},
				{Query: "SELECT id FROM foo", Want: map[string][]sql.NullInt64{"*": newNullInts(1, 3)}},
				{},
				{Query: "INSERT INTO foo VALUES (2, 20)"},
			},
			{
				{Query: "BEGIN"},
				{Query: "SELECT id FROM foo WHERE id < 3", Want: map[string][]sql.NullInt64{"*": newNullInts(1)}},
				{Query: "SELECT id FROM foo WHERE id < 3", WantOK: map[string][]sql.NullInt64{"*": newNullInts(1)}, WantNG: map[string][]sql.NullInt64{"*": newNullInts(1, 2)}},
				{Query: "COMMIT"},
			},
		},
		Threshold: map[string]string{
			SQLSERVER: SERIALIZABLE,
			"*":       REPEATABLE_READ,
		},
		AdditionalOk: map[string][]string{
			SQLSERVER: {SNAPSHOT},
		},
		WantStarts: map[string][]string{"*": genSeq(3, 4)},
		WantEnds: map[string][]string{
			MYSQL + ":" + REPEATABLE_READ_LOCK: {"a:0", "b:0", "a:1", "b:1", "b:2", "b:3", "a:2"}, // INSERT is locked
			MYSQL + ":" + SERIALIZABLE:         {"a:0", "b:0", "a:1", "b:1", "b:2", "b:3", "a:2"}, // INSERT is locked
			SQLSERVER + ":" + SERIALIZABLE:     {"a:0", "b:0", "a:1", "b:1", "b:2", "b:3", "a:2"}, // INSERT is locked
			DB2 + ":" + REPEATABLE_READ:        {"a:0", "b:0", "a:1", "b:1", "b:2", "b:3", "a:2"}, // INSERT is locked
			DB2 + ":" + SERIALIZABLE:           {"a:0", "b:0", "a:1", "b:1", "b:2", "b:3", "a:2"}, // INSERT is locked
			SQLITE + ":" + SERIALIZABLE:        {"a:0", "b:0", "a:1", "b:1", "b:2", "b:3", "a:2"}, // INSERT is locked
			"*":                                genSeq(3, 4),
		},
	},

	{
		Name: "lost update",
		Txs: [][]query{
			{
				{Query: "BEGIN"},
				{Query: "SELECT value FROM foo WHERE id = 1", Want: map[string][]sql.NullInt64{"*": newNullInts(2)}},
				{Query: "UPDATE foo SET value = 3 WHERE id = 1 -- increment value", WantErr: map[string]string{
					POSTGRES + ":" + REPEATABLE_READ_LOCK: "ERROR: deadlock detected (SQLSTATE 40P01)",
				}},
				{Query: "COMMIT"},
				{}, // T2のコミットを待つ
				{Query: "SELECT value FROM foo WHERE id = 1",
					Want: map[string][]sql.NullInt64{
						POSTGRES + ":" + REPEATABLE_READ:  newNullInts(3),
						POSTGRES + ":" + SERIALIZABLE:     newNullInts(3),
						MYSQL + ":" + SERIALIZABLE:        newNullInts(3),
						SQLSERVER + ":" + REPEATABLE_READ: newNullInts(3),
						SQLSERVER + ":" + SNAPSHOT:        newNullInts(3),
						SQLSERVER + ":" + SERIALIZABLE:    newNullInts(3),
						ORACLE + ":" + SERIALIZABLE:       newNullInts(3),
						DB2 + ":" + READ_STABILITY:        newNullInts(3),
						DB2 + ":" + REPEATABLE_READ:       newNullInts(3),
						DB2 + ":" + SERIALIZABLE:          newNullInts(3),
					},
					WantOK: map[string][]sql.NullInt64{"*": newNullInts(4)},
					WantNG: map[string][]sql.NullInt64{"*": newNullInts(3)},
				},
			},
			{
				{Query: "BEGIN"},
				{Query: "SELECT value FROM foo WHERE id = 1", Want: map[string][]sql.NullInt64{"*": newNullInts(2)}},
				{Query: "UPDATE foo SET value = 3 WHERE id = 1 -- increment value",
					WantErr: map[string]string{
						MYSQL + ":" + REPEATABLE_READ_LOCK: "Error 1213: Deadlock found when trying to get lock; try restarting transaction",
						MYSQL + ":" + SERIALIZABLE:         "Error 1213: Deadlock found when trying to get lock; try restarting transaction",
						POSTGRES + ":" + REPEATABLE_READ:   "ERROR: could not serialize access due to concurrent update (SQLSTATE 40001)",
						POSTGRES + ":" + SERIALIZABLE:      "ERROR: could not serialize access due to concurrent update (SQLSTATE 40001)",
						SQLSERVER + ":" + REPEATABLE_READ:  "mssql: Transaction \\(Process ID \\d+\\) was deadlocked on lock resources with another process and has been chosen as the deadlock victim. Rerun the transaction\\.",
						SQLSERVER + ":" + SNAPSHOT:         "mssql: Snapshot isolation transaction aborted due to update conflict. You cannot use snapshot isolation to access table 'dbo.foo' directly or indirectly in database 'test2' to update, delete, or insert the row that has been modified or deleted by another transaction. Retry the transaction or change the isolation level for the update/delete statement.",
						SQLSERVER + ":" + SERIALIZABLE:     "mssql: Transaction \\(Process ID \\d+\\) was deadlocked on lock resources with another process and has been chosen as the deadlock victim. Rerun the transaction\\.",
						ORACLE + ":" + SERIALIZABLE:        "ORA-08177: can't serialize access for this transaction\n",
						DB2 + ":" + READ_STABILITY:         "SQLExecute: {40001} [IBM][CLI Driver][DB2/LINUXX8664] SQL0911N  The current transaction has been rolled back because of a deadlock or timeout.  Reason code \"2\".  SQLSTATE=40001\n",
						DB2 + ":" + REPEATABLE_READ:        "SQLExecute: {40001} [IBM][CLI Driver][DB2/LINUXX8664] SQL0911N  The current transaction has been rolled back because of a deadlock or timeout.  Reason code \"2\".  SQLSTATE=40001\n",
						DB2 + ":" + SERIALIZABLE:           "SQLExecute: {40001} [IBM][CLI Driver][DB2/LINUXX8664] SQL0911N  The current transaction has been rolled back because of a deadlock or timeout.  Reason code \"2\".  SQLSTATE=40001\n",
					},
					compile: map[string]bool{
						SQLSERVER + ":" + REPEATABLE_READ: true,
						SQLSERVER + ":" + SERIALIZABLE:    true,
					},
				},
				{Query: "COMMIT"},
				{Query: "SELECT value FROM foo WHERE id = 1",
					WantOK: map[string][]sql.NullInt64{"*": newNullInts(4)},
					WantNG: map[string][]sql.NullInt64{"*": newNullInts(3)},
				},
			},
		},
		Threshold: map[string]string{
			MYSQL: SERIALIZABLE,
			DB2:   READ_STABILITY,
			"*":   REPEATABLE_READ,
		},
		AdditionalOk: map[string][]string{
			POSTGRES:  {REPEATABLE_READ_LOCK},
			MYSQL:     {REPEATABLE_READ_LOCK},
			SQLSERVER: {REPEATABLE_READ},
		},
		WantStarts: map[string][]string{
			SERIALIZABLE:                              genSeq(6, 3),
			POSTGRES + ":" + REPEATABLE_READ:          genSeq(6, 3),
			POSTGRES + ":" + REPEATABLE_READ_LOCK:     genSeq(3, 5),
			MYSQL + ":" + REPEATABLE_READ_LOCK:        genSeq(6, 3),
			SQLSERVER + ":" + READ_UNCOMMITTED:        {"a:0", "b:0", "a:1", "b:1", "a:2", "b:2", "a:3", "a:4", "b:3", "a:5", "b:4"},
			SQLSERVER + ":" + READ_COMMITTED:          {"a:0", "b:0", "a:1", "b:1", "a:2", "b:2", "a:3", "a:4", "b:3", "a:5", "b:4"},
			SQLSERVER + ":" + READ_COMMITTED_SNAPSHOT: {"a:0", "b:0", "a:1", "b:1", "a:2", "b:2", "a:3", "a:4", "b:3", "a:5", "b:4"},
			SQLSERVER + ":" + REPEATABLE_READ:         genSeq(6, 3),
			SQLSERVER + ":" + SNAPSHOT:                genSeq(6, 3),
			DB2 + ":" + READ_STABILITY:                {"a:0", "b:0", "a:1", "b:1", "a:2", "b:2", "a:3", "a:4", "a:5"},
			DB2 + ":" + REPEATABLE_READ:               {"a:0", "b:0", "a:1", "b:1", "a:2", "b:2", "a:3", "a:4", "a:5"},
			DB2 + ":" + SERIALIZABLE:                  {"a:0", "b:0", "a:1", "b:1", "a:2", "b:2", "a:3", "a:4", "a:5"},
			NO_TRANSACTION:                            genSeq(6, 5),
			"*":                                       {"a:0", "b:0", "a:1", "b:1", "a:2", "b:2", "a:3", "a:4", "b:3", "a:5", "b:4"},
		},
		WantEnds: map[string][]string{
			NO_TRANSACTION:                            genSeq(6, 5),
			SERIALIZABLE:                              {"a:0", "b:0", "a:1", "b:1", "b:2", "a:2", "a:3", "a:4", "a:5"},
			POSTGRES + ":" + REPEATABLE_READ:          {"a:0", "b:0", "a:1", "b:1", "a:2", "a:3", "b:2", "a:4", "a:5"},
			POSTGRES + ":" + REPEATABLE_READ_LOCK:     {"a:0", "b:0", "a:1", "b:1", "b:2", "a:2", "b:3", "b:4", "a:5"},
			POSTGRES + ":" + SERIALIZABLE:             {"a:0", "b:0", "a:1", "b:1", "a:2", "a:3", "b:2", "a:4", "a:5"}, // same as POSTGRES:REPEATABLE_READ
			MYSQL + ":" + REPEATABLE_READ_LOCK:        {"a:0", "b:0", "a:1", "b:1", "b:2", "a:2", "a:3", "a:4", "a:5"},
			SQLSERVER + ":" + READ_COMMITTED:          {"a:0", "b:0", "a:1", "b:1", "a:2", "a:3", "b:2", "a:4", "b:3", "a:5", "b:4"},
			SQLSERVER + ":" + READ_COMMITTED_SNAPSHOT: {"a:0", "b:0", "a:1", "b:1", "a:2", "a:3", "b:2", "a:4", "b:3", "a:5", "b:4"},
			SQLSERVER + ":" + REPEATABLE_READ:         {"a:0", "b:0", "a:1", "b:1", "a:2", "b:2", "a:3", "a:4", "a:5"},
			SQLSERVER + ":" + SNAPSHOT:                {"a:0", "b:0", "a:1", "b:1", "a:2", "a:3", "b:2", "a:4", "a:5"}, // same as POSTGRES:REPEATABLE_READ
			ORACLE + ":" + SERIALIZABLE:               {"a:0", "b:0", "a:1", "b:1", "a:2", "a:3", "b:2", "a:4", "a:5"}, // same as POSTGRES:REPEATABLE_READ
			DB2 + ":" + READ_STABILITY:                {"a:0", "b:0", "a:1", "b:1", "b:2", "a:2", "a:3", "a:4", "a:5"},
			DB2 + ":" + REPEATABLE_READ:               {"a:0", "b:0", "a:1", "b:1", "b:2", "a:2", "a:3", "a:4", "a:5"},
			DB2 + ":" + SERIALIZABLE:                  {"a:0", "b:0", "a:1", "b:1", "b:2", "a:2", "a:3", "a:4", "a:5"},
			"*":                                       {"a:0", "b:0", "a:1", "b:1", "a:2", "a:3", "b:2", "a:4", "b:3", "a:5", "b:4"}, // 1:UPDATE is locked
		},
		Skip: map[string]bool{
			SQLITE + ":" + SERIALIZABLE: true, // "database is locked" won't finish transaction ?
		},
	},

	{
		Name: "write skew",
		Txs: [][]query{
			{
				{Query: "BEGIN"},
				{Query: "SELECT value FROM foo WHERE id = 1", Want: map[string][]sql.NullInt64{"*": newNullInts(2)}}, // get X
				{Query: "UPDATE foo SET value = 20 WHERE id = 3", // update Y to X*10
					WantErr: map[string]string{
						POSTGRES + ":" + REPEATABLE_READ_LOCK: "ERROR: deadlock detected (SQLSTATE 40P01)",
						DB2 + ":" + READ_STABILITY:            "SQLExecute: {40001} [IBM][CLI Driver][DB2/LINUXX8664] SQL0911N  The current transaction has been rolled back because of a deadlock or timeout.  Reason code \"2\".  SQLSTATE=40001\n",
						DB2 + ":" + REPEATABLE_READ:           "SQLExecute: {40001} [IBM][CLI Driver][DB2/LINUXX8664] SQL0911N  The current transaction has been rolled back because of a deadlock or timeout.  Reason code \"2\".  SQLSTATE=40001\n",
						DB2 + ":" + SERIALIZABLE:              "SQLExecute: {40001} [IBM][CLI Driver][DB2/LINUXX8664] SQL0911N  The current transaction has been rolled back because of a deadlock or timeout.  Reason code \"2\".  SQLSTATE=40001\n",
					},
				},
				{Query: "SELECT value FROM foo WHERE id = 3", Want: map[string][]sql.NullInt64{"*": newNullInts(20)}}, // got X*10
				{Query: "COMMIT"},
				{Query: "SELECT value FROM foo WHERE id = 1", WantOK: map[string][]sql.NullInt64{"*": newNullInts(2)}, WantNG: map[string][]sql.NullInt64{"*": newNullInts(40)}}, // T2 should be aborted and keep Y = X * 2
			},
			{
				{Query: "BEGIN"},
				{Query: "SELECT value FROM foo WHERE id = 3", Want: map[string][]sql.NullInt64{"*": newNullInts(4)}}, // get Y
				// update X to Y*10
				{Query: "UPDATE foo SET value = 40 WHERE id = 1",
					WantErr: map[string]string{
						MYSQL + ":" + REPEATABLE_READ_LOCK: "Error 1213: Deadlock found when trying to get lock; try restarting transaction",
						MYSQL + ":" + SERIALIZABLE:         "Error 1213: Deadlock found when trying to get lock; try restarting transaction",
						SQLSERVER + ":" + REPEATABLE_READ:  "mssql: Transaction \\(Process ID \\d+\\) was deadlocked on lock resources with another process and has been chosen as the deadlock victim. Rerun the transaction\\.",
						SQLSERVER + ":" + SERIALIZABLE:     "mssql: Transaction \\(Process ID \\d+\\) was deadlocked on lock resources with another process and has been chosen as the deadlock victim. Rerun the transaction\\.",
					},
					compile: map[string]bool{
						SQLSERVER + ":" + REPEATABLE_READ: true,
						SQLSERVER + ":" + SERIALIZABLE:    true,
					}},
				// TODO:`Want:`を定義
				// Oracle SerializableがOK判定になってる
				{Query: "SELECT value FROM foo WHERE id = 1",
					Want: map[string][]sql.NullInt64{
						POSTGRES + ":" + SERIALIZABLE: newNullInts(40),
					},
					WantOK: map[string][]sql.NullInt64{"*": newNullInts(2)},
					WantNG: map[string][]sql.NullInt64{"*": newNullInts(40)},
				}, // write skew: now X=40, Y=20, so not Y = X*10 nor X != Y*10
				{Query: "COMMIT", WantErr: map[string]string{
					POSTGRES + ":" + SERIALIZABLE: "ERROR: could not serialize access due to read/write dependencies among transactions (SQLSTATE 40001)",
				}},
				{Query: "SELECT value FROM foo WHERE id = 3", WantOK: map[string][]sql.NullInt64{"*": newNullInts(4)}, WantNG: map[string][]sql.NullInt64{"*": newNullInts(20)}}, // T1 should be aborted and keep Y = X * 2
			},
		},
		Threshold: map[string]string{
			ORACLE: NEVER,
			DB2:    READ_STABILITY,
			"*":    SERIALIZABLE,
		},
		AdditionalOk: map[string][]string{
			POSTGRES:  {REPEATABLE_READ_LOCK},
			MYSQL:     {REPEATABLE_READ_LOCK},
			SQLSERVER: {REPEATABLE_READ},
		},
		WantStarts: map[string][]string{
			POSTGRES + ":" + SERIALIZABLE:         genSeq(6, 5),
			POSTGRES + ":" + REPEATABLE_READ_LOCK: genSeq(3, 6),
			MYSQL + ":" + REPEATABLE_READ_LOCK:    genSeq(6, 3),
			MYSQL + ":" + SERIALIZABLE:            genSeq(6, 3),
			SQLSERVER + ":" + REPEATABLE_READ:     genSeq(6, 3),
			SQLSERVER + ":" + SERIALIZABLE:        genSeq(6, 3),
			DB2 + ":" + READ_STABILITY:            {"a:0", "b:0", "a:1", "b:1", "a:2", "b:2", "b:3", "b:4", "b:5"},
			DB2 + ":" + REPEATABLE_READ:           {"a:0", "b:0", "a:1", "b:1", "a:2", "b:2", "b:3", "b:4", "b:5"},
			DB2 + ":" + SERIALIZABLE:              {"a:0", "b:0", "a:1", "b:1", "a:2", "b:2", "b:3", "b:4", "b:5"},
			"*":                                   genSeq(6, 6),
		},
		WantEnds: map[string][]string{
			POSTGRES + ":" + REPEATABLE_READ_LOCK: {"a:0", "b:0", "a:1", "b:1", "b:2", "a:2", "b:3", "b:4", "b:5"},
			POSTGRES + ":" + SERIALIZABLE:         genSeq(6, 5),                                                    // abort on T2 commit
			MYSQL + ":" + REPEATABLE_READ_LOCK:    {"a:0", "b:0", "a:1", "b:1", "b:2", "a:2", "a:3", "a:4", "a:5"}, // query 0:2 is locked, query1:2 crashes
			MYSQL + ":" + SERIALIZABLE:            {"a:0", "b:0", "a:1", "b:1", "b:2", "a:2", "a:3", "a:4", "a:5"}, // query 0:2 is locked, query1:2 crashes
			SQLSERVER + ":" + REPEATABLE_READ:     {"a:0", "b:0", "a:1", "b:1", "a:2", "b:2", "a:3", "a:4", "a:5"}, // query 0:2 is locked, query1:2 crashes
			SQLSERVER + ":" + SERIALIZABLE:        {"a:0", "b:0", "a:1", "b:1", "a:2", "b:2", "a:3", "a:4", "a:5"}, // query 0:2 is locked, query1:2 crashes
			DB2 + ":" + READ_STABILITY:            {"a:0", "b:0", "a:1", "b:1", "a:2", "b:2", "b:3", "b:4", "b:5"},
			DB2 + ":" + REPEATABLE_READ:           {"a:0", "b:0", "a:1", "b:1", "a:2", "b:2", "b:3", "b:4", "b:5"},
			DB2 + ":" + SERIALIZABLE:              {"a:0", "b:0", "a:1", "b:1", "a:2", "b:2", "b:3", "b:4", "b:5"},
			"*":                                   genSeq(6, 6),
		},
		Skip: map[string]bool{
			SQLITE + ":" + SERIALIZABLE: true, // "database is locked" won't finish transaction ?
		},
	},
}

func TestMain(m *testing.M) {
	ctx := context.Background()
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(15*time.Second))
	defer cancel()

	db, err := sql.Open("sqlserver", "server=127.0.0.1;user id=SA;password=Passw0rd")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	conn, err := db.Conn(ctx)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	// test1
	_, err = conn.ExecContext(ctx, "DROP DATABASE IF EXISTS test1")
	if err != nil {
		panic(err)
	}
	_, err = conn.ExecContext(ctx, "CREATE DATABASE test1")
	if err != nil {
		panic(err)
	}

	// test2
	_, err = conn.ExecContext(ctx, "DROP DATABASE IF EXISTS test2")
	if err != nil {
		panic(err)
	}
	_, err = conn.ExecContext(ctx, "CREATE DATABASE test2")
	if err != nil {
		panic(err)
	}
	_, err = conn.ExecContext(ctx, "ALTER DATABASE test2 SET ALLOW_SNAPSHOT_ISOLATION ON")
	if err != nil {
		panic(err)
	}
	_, err = conn.ExecContext(ctx, "ALTER DATABASE test2 SET READ_COMMITTED_SNAPSHOT ON")
	if err != nil {
		panic(err)
	}

	os.Exit(m.Run())
}

func Test(t *testing.T) {
	buf, err := json.MarshalIndent(specs, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	err = os.WriteFile("./specs.json", buf, 0644)
	if err != nil {
		t.Fatal(err)
	}

	type test struct {
		database     string
		level        string
		name         string
		txs          [][]query
		threshold    map[string]string
		additionalOk map[string][]string
		wantStarts   map[string][]string
		wantEnds     map[string][]string
		skip         map[string]bool
	}

	tests := make([]test, 0)

	for _, database := range databases {
		levels := dbLevels["*"]
		if v, ok := dbLevels[database]; ok {
			levels = v
		}

		for _, level := range levels {
			for _, spec := range specs {
				tests = append(tests, test{
					database:     database,
					level:        level,
					name:         spec.Name,
					txs:          spec.Txs,
					threshold:    spec.Threshold,
					additionalOk: spec.AdditionalOk,
					wantStarts:   spec.WantStarts,
					wantEnds:     spec.WantEnds,
					skip:         spec.Skip,
				})
			}
		}
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s/%s/%s", tt.database, tt.level, tt.name), func(t *testing.T) {
			skip := tt.skip["*"]
			if v, ok := tt.skip[tt.database]; ok {
				skip = v
			}
			if v, ok := tt.skip[tt.level]; ok {
				skip = v
			}
			if v, ok := tt.skip[tt.database+":"+tt.level]; ok {
				skip = v
			}

			if skip {
				t.SkipNow()
			}
			if tt.database == SQLITE && (tt.level != NO_TRANSACTION && tt.level != SERIALIZABLE) {
				t.SkipNow()
			}

			ctx := context.Background()
			ctx, cancel := context.WithDeadline(ctx, time.Now().Add(15*time.Second))
			defer cancel()

			db, err := openDB(tt.database, tt.level)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()

			conn, err := db.Conn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()

			fmt.Println("DROP TABLE foo")
			_, _ = conn.ExecContext(ctx, "DROP TABLE foo")
			// ignore drop table error
			// if err != nil {
			// 	t.Fatal(err)
			// }
			fmt.Println("CREATE TABLE foo (id INT NOT NULL PRIMARY KEY, value INT)")
			_, err = conn.ExecContext(ctx, "CREATE TABLE foo (id INT NOT NULL PRIMARY KEY, value INT)")
			if err != nil {
				t.Fatal(err)
			}
			fmt.Println("INSERT INTO foo VALUES (1, 2)")
			_, err = conn.ExecContext(ctx, "INSERT INTO foo VALUES (1, 2)")
			if err != nil {
				t.Fatal(err)
			}
			// oracle doesn't support `INSERT INTO foo VALUES (1, 2), (3, 4)`
			fmt.Println("INSERT INTO foo VALUES (3, 4)")
			_, err = conn.ExecContext(ctx, "INSERT INTO foo VALUES (3, 4)")
			if err != nil {
				t.Fatal(err)
			}

			threshold := tt.threshold["*"]
			if v, ok := tt.threshold[tt.database]; ok {
				threshold = v
			}
			ok := levelInt[tt.level] >= levelInt[threshold]
			if slices.Contains(tt.additionalOk[tt.database], tt.level) {
				ok = true
			}
			txs := make([][]transactonstest.Query, len(tt.txs))
			for i, queries := range tt.txs {
				txs[i] = make([]transactonstest.Query, len(tt.txs[i]))
				for j, q := range queries {
					query := q.Query
					if query == "BEGIN" {
						query = startTransaction(tt.database, tt.level, i)
					} else if query == "COMMIT" {
						query = commit(tt.database, tt.level)
					} else if query == "ROLLBACK" {
						query = rollback(tt.database, tt.level)
					}

					if query == "" {
						query = "SELECT 1"
						if tt.database == ORACLE {
							query = "SELECT 1 FROM dual"
						} else if tt.database == DB2 {
							query = "SELECT 1 FROM SYSIBM.DUAL"
						}
					}

					if tt.level == REPEATABLE_READ_LOCK && strings.HasPrefix(query, "SELECT") {
						query = query + " FOR SHARE"
					}

					if tt.database == SQLSERVER && strings.Contains(query, " FOR UPDATE") {
						query = strings.ReplaceAll(query, " FOR UPDATE", "")
						query = strings.ReplaceAll(query, " FROM foo", " FROM foo WITH(ROWLOCK, UPDLOCK)")
					}

					want := q.Want["*"]
					if v, ok := q.Want[tt.database+":"+tt.level]; ok {
						want = v
					} else if v, ok := q.Want[tt.level]; ok {
						want = v
					} else if v, ok := q.Want[tt.database]; ok {
						want = v
					}

					var wantOK []sql.NullInt64
					if want == nil {
						wantOK = q.WantOK["*"]
						if v, ok := q.WantOK[tt.database+":"+tt.level]; ok {
							wantOK = v
						} else if v, ok := q.WantOK[tt.level]; ok {
							wantOK = v
						} else if v, ok := q.WantOK[tt.database]; ok {
							wantOK = v
						}
					}

					var wantNG []sql.NullInt64
					if want == nil {
						wantNG = q.WantNG["*"]
						if v, ok := q.WantNG[tt.database+":"+tt.level]; ok {
							wantNG = v
						} else if v, ok := q.WantNG[tt.level]; ok {
							wantNG = v
						} else if v, ok := q.WantNG[tt.database]; ok {
							wantNG = v
						}
					}

					txs[i][j] = transactonstest.Query{
						Query:   query,
						Want:    want,
						WantOK:  wantOK,
						WantNG:  wantNG,
						WantErr: q.WantErr[tt.database+":"+tt.level],
						Compile: q.compile[tt.database+":"+tt.level],
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

			isolationLevel := getIsolationLevel(tt.database, tt.level)

			fmt.Printf("threshold: %s, %d > %d => ok: %t\n", threshold, levelInt[tt.level], levelInt[threshold], ok)
			fmt.Printf("%+v\n", txs)
			transactonstest.RunTransactionsTest(t, ctx, db, isolationLevel, txs, wantStarts, wantEnds, ok)
		})
	}
}
