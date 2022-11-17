package zakodb0

import (
	"encoding/json"
	"fmt"

	"github.com/yokomotod/zakodb/internal/common"
	"golang.org/x/exp/slices"
)

type ZakoDB0 struct {
	tables map[string]*table0
}

type table0 struct {
	name    string
	columns []ColumnSchema0
	rows    []map[string]int
}
type ColumnSchema0 struct {
	Name string
}

var _ common.ZakoDB = (*ZakoDB0)(nil)

func NewZakoDB() *ZakoDB0 {
	return &ZakoDB0{
		tables: make(map[string]*table0),
	}
}

type CreateTableQuery0 struct {
	Name   string
	Schema []ColumnSchema0
}

func (db *ZakoDB0) CreateTable(q CreateTableQuery0) error {
	if db.tables[q.Name] != nil {
		return fmt.Errorf("table '%s' already exists", q.Name)
	}

	db.tables[q.Name] = &table0{
		name:    q.Name,
		columns: q.Schema,
		rows:    make([]map[string]int, 0),
	}

	return nil
}

func (db *ZakoDB0) getTable(table string) (*table0, error) {
	tb := db.tables[table]
	if tb == nil {
		return nil, fmt.Errorf("table '%s' doesn't exist", table)
	}

	return tb, nil
}

type SelectQuery0 struct {
	Table string
}

func (db *ZakoDB0) Select(q SelectQuery0) ([][]any, []string, error) {
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
		rows[i] = make([]any, len(tb.columns))
		for j, col := range tb.columns {
			rows[i][j] = row[col.Name]
		}
	}

	return rows, cols, nil
}

type InsertQuery0 struct {
	Table  string
	Values []map[string]int
}

func (db *ZakoDB0) Insert(q InsertQuery0) error {
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
		tb.rows = append(tb.rows, row)
	}

	return nil
}

func Match(row map[string]int, where map[string]int) (bool, error) {
	for k, v1 := range where {
		v2, ok := row[k]
		if !ok {
			return false, fmt.Errorf("unknown column '%s' in where clause", k)
		}

		if v2 != v1 {
			return false, nil
		}
	}

	return true, nil
}

type UpdateQuery0 struct {
	Table string
	Where map[string]int
	Set   map[string]int
}

func (db *ZakoDB0) Update(q UpdateQuery0) error {
	tb, err := db.getTable(q.Table)
	if err != nil {
		return err
	}

	for _, row := range tb.rows {
		matched, err := Match(row, q.Where)
		if err != nil {
			return err
		}
		if !matched {
			continue
		}
		for k, v := range q.Set {
			if _, ok := row[k]; !ok {
				return fmt.Errorf("unknown column '%s' in field list", k)
			}
			row[k] = v
		}
	}

	return nil
}

type DeleteQuery0 struct {
	Table string
	Where map[string]int
}

func (db *ZakoDB0) Delete(q DeleteQuery0) error {
	tb, err := db.getTable(q.Table)
	if err != nil {
		return err
	}

	for i, row := range tb.rows {
		matched, err := Match(row, q.Where)
		if err != nil {
			return err
		}
		if matched {
			tb.rows = slices.Delete(tb.rows, i, i+1)
		}
	}

	return nil
}

type QueryType struct {
	Type string
}

func (db *ZakoDB0) Handle(s string) ([][]any, []string, error) {
	b := []byte(s)
	var q QueryType
	err := json.Unmarshal(b, &q)
	if err != nil {
		return nil, nil, fmt.Errorf("json.Unmarshal:%w", err)
	}

	switch q.Type {
	case "create_table":
		var q CreateTableQuery0
		err = json.Unmarshal(b, &q)
		if err != nil {
			return nil, nil, fmt.Errorf("json.Unmarshal:%w", err)
		}
		err := db.CreateTable(q)
		return nil, nil, err
	case "select":
		var q SelectQuery0
		err = json.Unmarshal(b, &q)
		if err != nil {
			return nil, nil, fmt.Errorf("json.Unmarshal:%w", err)
		}
		return db.Select(q)
	case "insert":
		var q InsertQuery0
		err = json.Unmarshal(b, &q)
		if err != nil {
			return nil, nil, fmt.Errorf("json.Unmarshal:%w", err)
		}
		err := db.Insert(q)
		return nil, nil, err
	case "update":
		var q UpdateQuery0
		err = json.Unmarshal(b, &q)
		if err != nil {
			return nil, nil, fmt.Errorf("json.Unmarshal:%w", err)
		}
		err := db.Update(q)
		return nil, nil, err
	case "delete":
		var q DeleteQuery0
		err = json.Unmarshal(b, &q)
		if err != nil {
			return nil, nil, fmt.Errorf("json.Unmarshal:%w", err)
		}
		err := db.Delete(q)
		return nil, nil, err
	case "begin":
		fallthrough
	case "commit":
		fallthrough
	case "rollback":
		return nil, nil, nil
	default:
		return nil, nil, fmt.Errorf("unknown type: %v", q.Type)
	}
}
