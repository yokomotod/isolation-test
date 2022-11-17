package zakodb1

import (
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/yokomotod/zakodb/internal/common"
	"github.com/yokomotod/zakodb/internal/zakodb0"
	"golang.org/x/exp/slices"
)

type ZakoDB1 struct {
	tables   map[string]*table1
	lastTxId uint64
}

type table1 struct {
	name    string
	columns []zakodb0.ColumnSchema0
	rows    []*row1
}

type row1 struct {
	cols            map[string]int
	colsUncommitted map[string]int
	lock            sync.Mutex
	txId            uint64
}

var _ common.ZakoDB = (*ZakoDB1)(nil)

func NewZakoDB() *ZakoDB1 {
	return &ZakoDB1{
		tables: make(map[string]*table1),
	}
}

func (db *ZakoDB1) CreateTable(q zakodb0.CreateTableQuery0) error {
	if db.tables[q.Name] != nil {
		return fmt.Errorf("table '%s' already exists", q.Name)
	}

	db.tables[q.Name] = &table1{
		name:    q.Name,
		columns: q.Schema,
		rows:    make([]*row1, 0),
	}

	return nil
}

func (db *ZakoDB1) getTable(table string) (*table1, error) {
	tb := db.tables[table]
	if tb == nil {
		return nil, fmt.Errorf("table '%s' doesn't exist", table)
	}

	return tb, nil
}

type SelectQuery1 struct {
	zakodb0.SelectQuery0
	TxId uint64
}

func (db *ZakoDB1) Select(q SelectQuery1) ([][]any, []string, error) {
	tb, err := db.getTable(q.Table)
	if err != nil {
		return nil, nil, err
	}

	cols := make([]string, len(tb.columns))
	for i, col := range tb.columns {
		cols[i] = col.Name
	}

	rows := make([][]any, len(tb.rows))
	for i, row := range tb.rows {
		cols := row.cols
		if row.colsUncommitted != nil && row.txId == q.TxId {
			cols = row.colsUncommitted
		}

		rows[i] = make([]any, len(tb.columns))
		for j, col := range tb.columns {
			rows[i][j] = cols[col.Name]
		}
	}

	return rows, cols, nil
}

func (db *ZakoDB1) Insert(q zakodb0.InsertQuery0) error {
	tb, err := db.getTable(q.Table)
	if err != nil {
		return err
	}

	for _, value := range q.Values {
		row := make(map[string]int)
		for _, col := range tb.columns {
			v, ok := value[col.Name]
			if !ok {
				return fmt.Errorf("no '%s'", col.Name)
			}
			row[col.Name] = v
		}
		tb.rows = append(tb.rows, &row1{cols: row})
	}

	return nil
}

type UpdateQuery1 struct {
	zakodb0.UpdateQuery0
	TxId uint64
}

func (db *ZakoDB1) Update(q UpdateQuery1) error {
	tb, err := db.getTable(q.Table)
	if err != nil {
		return err
	}

	for i, row := range tb.rows {
		matched, err := zakodb0.Match(row.cols, q.Where)
		if err != nil {
			return err
		}
		if !matched {
			continue
		}

		fmt.Printf("txId=%d: tables[%s][%d].txId=%d\n", q.TxId, q.Table, i, row.txId)
		locked := false
		if row.txId != q.TxId {
			fmt.Printf("txId=%d: lock tables[%s][%d]\n", q.TxId, q.Table, i)
			row.lock.Lock()
			row.txId = q.TxId
			locked = true
		}
		row.colsUncommitted = make(map[string]int, len(row.cols))
		for k, v := range row.cols {
			row.colsUncommitted[k] = v
		}

		for k, v := range q.Set {
			if _, ok := row.colsUncommitted[k]; !ok {
				return fmt.Errorf("unknown column '%s' in field list", k)
			}
			row.colsUncommitted[k] = v
		}

		if locked && q.TxId == 0 {
			fmt.Printf("txId=%d: unlock tables[%s][%d]\n", q.TxId, q.Table, i)
			row.lock.Unlock()
		}
	}

	return nil
}

type DeleteQuery1 struct {
	zakodb0.DeleteQuery0
	TxId uint64
}

func (db *ZakoDB1) Delete(q DeleteQuery1) error {
	tb, err := db.getTable(q.Table)
	if err != nil {
		return err
	}

	for i, row := range tb.rows {
		matched, err := zakodb0.Match(row.cols, q.Where)
		if err != nil {
			return err
		}
		if matched {
			if row.txId == 0 {
				row.lock.Lock()
			}
			tb.rows = slices.Delete(tb.rows, i, i+1)
			if q.TxId == 0 {
				row.lock.Unlock()
			}
		}
	}

	return nil
}

func (db *ZakoDB1) Begin() uint64 {
	atomic.AddUint64(&db.lastTxId, 1)
	return db.lastTxId
}

type CommitQuery1 struct {
	TxId uint64
}

func (db *ZakoDB1) Commit(q CommitQuery1) {
	for tableName, table := range db.tables {
		for id, row := range table.rows {
			if row.txId == q.TxId {
				fmt.Printf("txId=%d: commit tables[%s][%d]\n", q.TxId, tableName, id)
				for k, v := range row.colsUncommitted {
					row.cols[k] = v
				}
				row.colsUncommitted = nil
				row.txId = 0
				row.lock.Unlock()
			}
		}
	}
}

func (db *ZakoDB1) Rollback() error {
	return fmt.Errorf("not implemented")
}

func (db *ZakoDB1) Handle(s string) ([][]any, []string, error) {
	b := []byte(s)
	var q zakodb0.QueryType
	err := json.Unmarshal(b, &q)
	if err != nil {
		return nil, nil, fmt.Errorf("json.Unmarshal:%w", err)
	}

	switch q.Type {
	case "create_table":
		var q zakodb0.CreateTableQuery0
		err = json.Unmarshal(b, &q)
		if err != nil {
			return nil, nil, fmt.Errorf("json.Unmarshal:%w", err)
		}
		err := db.CreateTable(q)
		return nil, nil, err
	case "select":
		var q SelectQuery1
		err = json.Unmarshal(b, &q)
		if err != nil {
			return nil, nil, fmt.Errorf("json.Unmarshal:%w", err)
		}
		return db.Select(q)
	case "insert":
		var q zakodb0.InsertQuery0
		err = json.Unmarshal(b, &q)
		if err != nil {
			return nil, nil, fmt.Errorf("json.Unmarshal:%w", err)
		}
		db.Insert(q)
		return nil, nil, nil
	case "update":
		var q UpdateQuery1
		err = json.Unmarshal(b, &q)
		if err != nil {
			return nil, nil, fmt.Errorf("json.Unmarshal:%w", err)
		}
		db.Update(q)
		return nil, nil, nil
	case "delete":
		var q DeleteQuery1
		err = json.Unmarshal(b, &q)
		if err != nil {
			return nil, nil, fmt.Errorf("json.Unmarshal:%w", err)
		}
		db.Delete(q)
		return nil, nil, nil
	case "begin":
		_ /* txId */ = db.Begin()
		// return []byte(strconv.FormatUint(txId, 10)), nil
		return nil, nil, nil
	case "commit":
		var q CommitQuery1
		err = json.Unmarshal(b, &q)
		if err != nil {
			return nil, nil, fmt.Errorf("json.Unmarshal:%w", err)
		}
		db.Commit(q)
		return nil, nil, nil
	case "rollback":
		return nil, nil, db.Rollback()
	default:
		return nil, nil, fmt.Errorf("unknown type: %v", q.Type)
	}
}
