package zakodb_test

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/yokomotod/zakodb/internal/zakodb"
)

type LoggingZakoDB struct {
	db zakodb.ZakoDB

	BatchLogs   []string
	HandleLogs  []string
	HandledLogs []string
}

func (logging *LoggingZakoDB) Handle(in string) ([][]any, error) {
	logging.HandleLogs = append(logging.HandleLogs, string(in))

	out, _, err := logging.db.Handle(in)

	logging.HandledLogs = append(logging.HandledLogs, string(in))

	return out, err
}

func (logging *LoggingZakoDB) BatchHandle(ins []string) ([][][]any, error) {
	logging.BatchLogs = append(logging.BatchLogs, ins...)

	outs := make([][][]any, len(ins))

	for i, in := range ins {
		out, err := logging.Handle(in)
		if err != nil {
			return nil, err
		}

		if out != nil {
			outs[i] = out
		}
	}

	return outs, nil
}

// func (logging *LoggingZakoDB) AsyncBatchHandle(ins []string) chan struct {
// 	out [][]any
// 	err error
// } {
// 	logging.BatchLogs = append(logging.BatchLogs, ins...)

// 	ch := make(chan struct {
// 		out [][]any
// 		err error
// 	})

// 	go func() {
// 		for _, in := range ins {
// 			out, err := logging.Handle(in)
// 			ch <- struct {
// 				out [][]any
// 				err error
// 			}{out: out, err: err}
// 		}

// 		close(ch)
// 	}()

// 	return ch
// }

// func (logging *LoggingZakoDB) AwaitBatchHandle(ch chan struct {
// 	out [][]any
// 	err error
// }) ([][][]any, error) {
// 	outs := make([][][]any, 0)

// 	for {
// 		res, ok := <-ch
// 		if !ok {
// 			break
// 		}
// 		if res.err != nil {
// 			return nil, res.err
// 		}

// 		outs = append(outs, res.out)
// 	}

// 	return outs, nil
// }

func NewLoggingZakoDB(name string) (*LoggingZakoDB, error) {
	zakoDB, err := zakodb.NewZakoDB(name)
	if err != nil {
		return nil, err
	}

	return &LoggingZakoDB{db: zakoDB}, nil
}

func assertExec(t *testing.T, db zakodb.ZakoDB, query string) {
	t.Helper()

	got, _, err := db.Handle(query)
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("%s", got)
	}
}

func assertQuery(t *testing.T, db zakodb.ZakoDB, query string, want any) {
	t.Helper()

	got, _, err := db.Handle(query)
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("assertBatch() mismatch (-want +got):\n%s", diff)
	}
}

func assertBatch(t *testing.T, db *LoggingZakoDB, queries []string, want any) {
	t.Helper()

	got, err := db.BatchHandle(queries)
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("assertBatch() mismatch (-want +got):\n%s", diff)
	}
}

// func assertAsyncBatch(t *testing.T, db *LoggingZakoDB, ch chan struct {
// 	out [][]any
// 	err error
// }, want [][][]any) {
// 	t.Helper()

// 	got, err := db.AwaitBatchHandle(ch)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	if diff := cmp.Diff(want, got); diff != "" {
// 		t.Errorf("assertAsyncBatch() mismatch (-want +got):\n%s", diff)
// 	}
// }

func TestNoTransaction(t *testing.T) {
	for _, name := range []string{
		"zakodb0",
		"zakodb1",
	} {
		t.Run(name, func(t *testing.T) {
			db, err := zakodb.NewZakoDB(name)
			if err != nil {
				t.Fatal(err)
			}

			assertExec(t, db, `{"type":"create_table","name":"foo","schema":[{"name":"a"},{"name":"b"}]}`)

			assertExec(t, db, `{"type":"insert","table":"foo","values":[{"a":1,"b":2},{"a":3,"b":4}]}`)
			assertQuery(t, db, `{"type":"select","table":"foo"}`, [][]any{{1, 2}, {3, 4}})

			assertExec(t, db, `{"type":"update","table":"foo","where":{"a":3,"b":4},"set":{"a":30,"b":40}}`)
			assertQuery(t, db, `{"type":"select","table":"foo"}`, [][]any{{1, 2}, {30, 40}})

			assertExec(t, db, `{"type":"delete","table":"foo","where":{"a":1,"b":2}}`)
			assertQuery(t, db, `{"type":"select","table":"foo"}`, [][]any{{30, 40}})
		})
	}
}

func TestDirtyRead(t *testing.T) {
	tests := []struct {
		name          string
		willDirtyRead bool
	}{
		{name: "zakodb0", willDirtyRead: true},
		{name: "zakodb1", willDirtyRead: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, err := zakodb.NewZakoDB(tt.name)
			if err != nil {
				t.Fatal(err)
			}

			assertExec(t, db, `{"type":"create_table","name":"foo","schema":[{"name":"a"},{"name":"b"}]}`)

			assertExec(t, db, `{"type":"insert","table":"foo","values":[{"a":1,"b":2},{"a":3,"b":4}]}`)
			assertQuery(t, db, `{"type":"select","table":"foo"}`, [][]any{{1, 2}, {3, 4}})

			assertExec(t, db, `{"type":"update","txId":1, "table":"foo","where":{"a":3,"b":4},"set":{"a":30,"b":40}}`)
			assertQuery(t, db, `{"type":"select","txId":1, "table":"foo"}`, [][]any{{1, 2}, {30, 40}})

			if tt.willDirtyRead {
				assertQuery(t, db, `{"type":"select","table":"foo"}`, [][]any{{1, 2}, {30, 40}})
			} else {
				assertQuery(t, db, `{"type":"select","table":"foo"}`, [][]any{{1, 2}, {3, 4}})
			}

			assertExec(t, db, `{"type":"commit", "txId":1}`)
			assertQuery(t, db, `{"type":"select", "table":"foo"}`, [][]any{{1, 2}, {30, 40}})
		})
	}
}

func TestDirtyWrite(t *testing.T) {
	prev := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(prev)

	db, err := NewLoggingZakoDB("zakodb0")
	if err != nil {
		t.Fatal(err)
	}

	q0 := `{"type":"create_table","name":"foo","schema":[{"name":"a"},{"name":"b"}]}`
	q1 := `{"type":"insert","table":"foo","values":[{"a":1,"b":2},{"a":3,"b":4}]}`
	q2 := `{"type":"select","table":"foo"}`
	r2 := [][]any{{1, 2}, {3, 4}}
	assertBatch(t, db, []string{q0, q1, q2}, [][][]any{nil, nil, r2})

	tx1q1 := `{"type":"update","txId":1, "table":"foo","where":{"a":1},"set":{"b":20}}`
	tx1q2 := `{"type":"update","txId":1, "table":"foo","where":{"a":3},"set":{"b":40}}`
	tx1q3 := `{"type":"select","txId":1, "table":"foo"}`
	tx1r3 := [][]any{{1, 20}, {3, 40}}
	tx1q4 := `{"type":"commit", "txId":1}`

	tx2q1 := `{"type":"update","txId":2, "table":"foo","where":{"a":1},"set":{"b":200}}`
	tx2q2 := `{"type":"update","txId":2, "table":"foo","where":{"a":3},"set":{"b":400}}`
	tx2q3 := `{"type":"select","txId":2, "table":"foo"}`
	tx2r3 := [][]any{{1, 200}, {3, 400}}
	tx2q4 := `{"type":"commit", "txId":2}`

	c1 := make(chan struct{})
	go func() {
		defer close(c1)

		// 1
		fmt.Println("tx1 start1")
		assertBatch(t, db, []string{tx1q1}, [][][]any{nil})
		fmt.Println("tx1 end1")

		c1 <- struct{}{}

		// 3
		fmt.Println("tx1 start2")
		assertBatch(t, db, []string{tx1q2, tx1q3, tx1q4}, [][][]any{nil, tx1r3, nil})
		fmt.Println("tx1 end2")

	}()
	runtime.Gosched()

	c2 := make(chan struct{})
	go func() {
		defer close(c2)

		// 2
		fmt.Println("tx2 start1")
		assertBatch(t, db, []string{tx2q1, tx2q2, tx2q3, tx2q4}, [][][]any{nil, nil, tx2r3, nil})
		// ch := db.AsyncBatchHandle([]string{tx2q1, tx2q2, tx2q3, tx2q4})
		// time.Sleep(1 * time.Millisecond)
		// fmt.Printf("%#v\n", db.BatchLogs)
		fmt.Println("tx2 end1")

		// assertAsyncBatch(t, db, ch, [][][]any{nil, nil, tx2r3, nil})
	}()
	runtime.Gosched()

	running := true
	for {
		running = false
		deadlock := true
		for i, ch := range []chan struct{}{c1, c2} {
			runtime.Gosched()

			select {
			case _, ok := <-ch:
				if ok {
					running = true
					deadlock = false
				} else {
					fmt.Printf("ch%d done\n", i)
				}
			default:
				fmt.Printf("ch%d waiting\n", i)
				running = true
			}
		}

		if !running {
			break
		}

		if deadlock {
			panic("deadlock")
		}
	}

	q3 := `{"type":"select", "table":"foo"}`
	r3 := [][]any{{1, 200}, {3, 400}}
	assertBatch(t, db, []string{q3}, [][][]any{r3})

	want := []string{
		q0,
		q1,
		q2,
		tx1q1,
		tx2q1, // tx2 batched
		tx2q2,
		tx2q3,
		tx2q4,
		tx1q2,
		tx1q3,
		tx1q4,
		q3,
	}
	if diff := cmp.Diff(want, db.BatchLogs); diff != "" {
		t.Errorf("db.BatchLogs mismatch (-want +got):\n%s", diff)
	}

	want = []string{
		q0,
		q1,
		q2,
		tx1q1,
		tx2q1, // tx2 started but locked
		tx1q2,
		tx1q3,
		tx1q4,
		tx2q2,
		tx2q3,
		tx2q4,
		q3,
	}
	if diff := cmp.Diff(want, db.HandleLogs); diff != "" {
		t.Errorf("db.HandleLogs mismatch (-want +got):\n%s", diff)
	}

	want = []string{
		q0,
		q1,
		q2,
		tx1q1,
		tx1q2,
		tx1q3,
		tx1q4,
		tx2q1, // tx2 unlocked
		tx2q2,
		tx2q3,
		tx2q4,
		q3,
	}
	if diff := cmp.Diff(want, db.HandledLogs); diff != "" {
		t.Errorf("db.HandledLogs mismatch (-want +got):\n%s", diff)
	}
}
