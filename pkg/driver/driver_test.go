package zakodb_driver_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/google/go-cmp/cmp"
	_ "github.com/yokomotod/zakodb/pkg/driver"
)

func TestDriver(t *testing.T) {
	db, err := sql.Open("zakodb", "zakodb0")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.ExecContext(ctx, `{"type":"create_table","name":"foo","schema":[{"name":"a"},{"name":"b"}]}`)
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.ExecContext(ctx, `{"type":"insert","table":"foo","values":[{"a":1,"b":2}]}`)
	if err != nil {
		t.Fatal(err)
	}

	rows, err := db.QueryContext(ctx, `{"type":"select","table":"foo"}`)
	if err != nil {
		t.Fatal(err)
	}

	columns, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}

	if diff := cmp.Diff([]string{"a", "b"}, columns); diff != "" {
		t.Errorf("mismatch (-want +got):\n%s", diff)
	}

	type result = struct {
		A int
		B int
	}

	got := make([]result, 0)
	for rows.Next() {
		var o result
		err = rows.Scan(&o.A, &o.B)
		if err != nil {
			break
		}
		got = append(got, o)
	}

	want := []result{{A: 1, B: 2}}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("mismatch (-want +got):\n%s", diff)
	}
}
