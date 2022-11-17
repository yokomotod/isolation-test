package zakodb_driver

import (
	"database/sql"
	"database/sql/driver"
	"io"

	"github.com/yokomotod/zakodb/internal/zakodb"
)

func init() {
	sql.Register("zakodb", &ZakoDBDriver{})
}

type ZakoDBDriver struct{}

func (d *ZakoDBDriver) Open(name string) (driver.Conn, error) {
	db, err := zakodb.NewZakoDB(name)
	if err != nil {
		return nil, err
	}

	return &zakoDBConn{db: db}, nil
}

type zakoDBConn struct {
	db zakodb.ZakoDB
}

func (c *zakoDBConn) Prepare(query string) (driver.Stmt, error) {
	return &zakoDBStmt{conn: c, query: query}, nil
}

func (c *zakoDBConn) Close() error {
	return nil
}

func (c *zakoDBConn) Begin() (driver.Tx, error) {
	return nil, nil
}

type zakoDBStmt struct {
	conn  *zakoDBConn
	query string
}

func (s *zakoDBStmt) Close() error {
	return nil
}

func (s *zakoDBStmt) NumInput() int {
	return 0
}

func (s *zakoDBStmt) Exec(args []driver.Value) (driver.Result, error) {
	_, _, err := s.conn.db.Handle(s.query)
	return nil, err
}

func (s *zakoDBStmt) Query(args []driver.Value) (driver.Rows, error) {
	rows, cols, err := s.conn.db.Handle(s.query)
	if err != nil {
		return nil, err
	}

	return &zakoDBRows{rows: rows, cols: cols}, nil
}

type zakoDBRows struct {
	cols []string
	rows [][]any
	i    int
}

func (r *zakoDBRows) Columns() []string {
	return r.cols
}

func (r *zakoDBRows) Close() error {
	return nil
}

func (r *zakoDBRows) Next(dest []driver.Value) error {
	if r.i >= len(r.rows) {
		return io.EOF
	}

	for j := range r.cols {
		dest[j] = r.rows[r.i][j]
	}
	r.i += 1

	return nil
}
