package transactonstest

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"reflect"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

type Query struct {
	Query   string
	Want    []sql.NullInt64
	WantOK  []sql.NullInt64
	WantNG  []sql.NullInt64
	WantErr string
	Compile bool
}

func getGoID() int {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	idField := strings.Fields(strings.TrimPrefix(string(buf[:n]), "goroutine "))[0]
	id, err := strconv.Atoi(idField)
	if err != nil {
		panic(fmt.Sprintf("cannot get goroutine id: %v", err))
	}
	return id
}

var (
	stepSleep = 50 * time.Millisecond // `sqlserver/READ_UNCOMMITTED/lost_update_with_locking_read` が20msでもなおずれる
	waitSleep = 50 * time.Millisecond
)

var ab = []string{"a", "b"}

type ConnOrTx interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func RunTransactionsTest(t *testing.T, ctx context.Context, db *sql.DB, isolationLevel sql.IsolationLevel, txs [][]Query, wantStarts, wantEnds []string, wantOK bool) {
	prev := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(prev)

	logger := log.New(os.Stdout, "", log.Ltime|log.Lmicroseconds)

	gotStarts := make([]string, 0)
	gotEnds := make([]string, 0)
	gotOKs := make([]bool, len(txs))

	channels := make([]chan struct{}, len(txs))

	ran := -1

	for i := range txs {
		channels[i] = make(chan struct{})

		var conn ConnOrTx
		conn, err := db.Conn(ctx)
		if err != nil {
			panic(err)
		}

		i := i

		go func() {
			goID := getGoID()

			ch := channels[i]
			defer close(ch)

			queries := txs[i]

			ok := true

			for j, q := range queries {
				ch <- struct{}{} // 1つ目のクエリを並行実行してしまわないように最後ではなく最初に同期

				logger.Printf("(go %d) start %s>[%d] %s\n", goID, ab[i], j, q.Query)
				gotStarts = append(gotStarts, fmt.Sprintf("%s:%d", ab[i], j))
				// start := time.Now()
				ran = i
				var rows *sql.Rows
				if strings.Contains(q.Query, "BEGIN") {
					qs := strings.Split(q.Query, "; ")
					for _, q := range qs {
						if strings.HasPrefix(q, "BEGIN") {
							conn, err = db.BeginTx(ctx, &sql.TxOptions{Isolation: isolationLevel})
						} else {
							_, err = conn.ExecContext(ctx, q)
						}

						if err != nil {
							panic(err)
						}
					}
				} else if strings.HasPrefix(q.Query, "SELECT") {
					rows, err = conn.QueryContext(ctx, q.Query)
				} else {
					// go-ora requires to use `Exec`
					// https://github.com/sijms/go-ora/issues/201
					_, err = conn.ExecContext(ctx, q.Query)
				}
				ch <- struct{}{} // 結果を待つために同期
				logger.Printf("(go %d) end   %s<[%d] %s\n", goID, ab[i], j, q.Query)

				want := q.Want
				if want == nil {
					if wantOK {
						want = q.WantOK
					} else {
						want = q.WantNG
					}
				}

				if err != nil {
					if q.WantErr != "" {
						matched := err.Error() == q.WantErr
						if q.Compile {
							var e error
							matched, e = regexp.MatchString("^"+q.WantErr+"$", err.Error())
							if e != nil {
								panic(e)
							}
						}
						if matched {
							// ok, but break
							logger.Printf("(go %d) err   %s<[%d] %s\n", goID, ab[i], j, err)
						} else {
							fmt.Println("error mismatch")
							fmt.Println(q.WantErr)
							fmt.Printf("%#v\n", err.Error())
							panic(err)
						}
					} else if err == sql.ErrNoRows && want == nil {
						// ok
					} else {
						fmt.Println("unexpected error")
						fmt.Println(err)
						panic(err)
					}
				} else {
					got := make([]sql.NullInt64, 0)
					if rows != nil {
						for rows.Next() {
							var c sql.NullInt64
							err = rows.Scan(&c)
							if err != nil {
								break
							}
							got = append(got, c)
						}
					}
					logger.Printf("(go %d) got   %s<[%d] %+v\n", goID, ab[i], j, got)

					if want != nil && !reflect.DeepEqual(got, want) {
						t.Errorf("query %s:%d got=%+v, want=%+v", ab[i], j, got, want)
					}
					if q.WantErr != "" {
						t.Errorf("query %s:%d got=%+v, wantErr=%s", ab[i], j, got, q.WantErr)
					}
					if q.WantNG != nil && reflect.DeepEqual(got, q.WantNG) {
						logger.Printf("query %s:%d got=%+v, wantNG=%+v", ab[i], j, got, q.WantNG)
						ok = false
					}
				}

				if ran != i {
					// additional sleep after lock
					logger.Printf("(go %d) additional sleep after lock\n", goID)
					time.Sleep(stepSleep)
				}
				logger.Printf("(go %d) append %s<[%d] %s\n", goID, ab[i], j, q.Query)
				gotEnds = append(gotEnds, fmt.Sprintf("%s:%d", ab[i], j))
				if err != nil && err != sql.ErrNoRows {
					break
				}
			}

			logger.Printf("(go %d) ok: %t\n", goID, ok)
			gotOKs[i] = ok
		}()
		// logger.Printf("tx%d runtime.Gosched()\n", i)
		runtime.Gosched()
	}

	// time.Sleep(1 * time.Second)
	running := true
	for {
		running = false
		// deadlock := true
		for i, ch := range channels {
			// logger.Printf("ch%d Gosched()\n", i)
			runtime.Gosched()
			// time.Sleep(sleepMs)

			logger.Printf("ch%d step\n", i)

			select {
			case _, ok := <-ch:
				if ok {
					logger.Printf("ch%d stepped\n", i)
					running = true
					// deadlock = false
					select {
					case <-ch:
						logger.Printf("ch%d 2nd stepped\n", i)
						time.Sleep(stepSleep)
					case <-time.After(waitSleep):
						logger.Printf("ch%d 2nd timeout\n", i)
					}
				} else {
					logger.Printf("ch%d done\n", i)
				}
			case <-time.After(waitSleep):
				logger.Printf("ch%d waiting\n", i)
				running = true
			}
		}

		if !running {
			break
		}

		// if deadlock {
		// 	panic("deadlock")
		// }

		select {
		case <-ctx.Done():
			panic(ctx.Err())
		default:
		}
	}

	if diff := cmp.Diff(wantStarts, gotStarts); diff != "" {
		t.Errorf("gotStarts mismatch (-want +got):\n%s\ngot: %#v", diff, gotStarts)
	}

	if wantEnds != nil {
		if diff := cmp.Diff(wantEnds, gotEnds); diff != "" {
			t.Errorf("gotEnds mismatch (-want +got):\n%s\ngot: %#v", diff, gotEnds)
		}
	}

	gotOK := true
	for _, ok := range gotOKs {
		if !ok {
			gotOK = false
		}
	}
	if gotOK != wantOK {
		t.Errorf("gotOK: %t but wantOK: %t", gotOK, wantOK)
	}
}
