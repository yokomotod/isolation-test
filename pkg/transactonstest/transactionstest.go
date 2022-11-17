package transactonstest

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

type Query struct {
	Query string
	Want  *sql.NullInt64
}

func getGoID() int {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	// fmt.Printf("%s\n", buf)
	idField := strings.Fields(strings.TrimPrefix(string(buf[:n]), "goroutine "))[0]
	id, err := strconv.Atoi(idField)
	if err != nil {
		panic(fmt.Sprintf("cannot get goroutine id: %v", err))
	}
	return id
}

var (
	stepSleep = 10 * time.Millisecond
	waitSleep = 50 * time.Millisecond
)

func RunTransactionsTest(t *testing.T, ctx context.Context, db *sql.DB, txs [][]Query, wantStarts, wantEnds []string) {
	prev := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(prev)
	// logger.Printf("GOMAXPROCS: %d\n", runtime.GOMAXPROCS(0))

	logger := log.New(os.Stdout, "", log.Ltime|log.Lmicroseconds)

	gotStarts := make([]string, 0)
	gotEnds := make([]string, 0)

	channels := make([]chan struct{}, len(txs))

	ran := -1

	for i := range txs {
		// logger.Printf("start ch%d\n", i)
		channels[i] = make(chan struct{})

		// logger.Printf("tx%d BeginTx\n", i)
		// tx, err := db.BeginTx(ctx, nil)
		conn, err := db.Conn(ctx)
		if err != nil {
			panic(err)
		}
		// _, err = conn.ExecContext(ctx, "SELECT 1") // ping
		// if err != nil {
		// 	panic(err)
		// }

		i := i

		go func() {
			// debug.PrintStack()
			goID := getGoID()
			// fmt.Println("goroutine started")
			// conn := conn

			ch := channels[i]
			defer close(ch)

			queries := txs[i]

			for j, q := range queries {
				ch <- struct{}{} // 1つ目のクエリを並行実行してしまわないように最後ではなく最初に同期

				logger.Printf("(go %d) start %d>[%d] %s\n", goID, i, j, q.Query)
				gotStarts = append(gotStarts, fmt.Sprintf("%d:%d", i, j))
				// start := time.Now()
				ran = i
				var got sql.NullInt64
				err := conn.QueryRowContext(ctx, q.Query).Scan(&got)
				ch <- struct{}{} // 結果を待つために同期
				logger.Printf("(go %d) end   %d<[%d] %s\n", goID, i, j, q.Query)
				if err != nil {
					if err != sql.ErrNoRows {
						t.Error(err)
						break
					}
					if err == sql.ErrNoRows && q.Want != nil {
						panic(fmt.Errorf("query %d:%d %w", i, j, err))
					}
				}
				if q.Want != nil && got != *q.Want {
					t.Errorf("query %d:%d got=%+v, want=%+v", i, j, got, q.Want)
				}

				// ch <- struct{}{}
				// if time.Since(start) > 10*time.Millisecond {
				if ran != i {
					// additional sleep after lock
					logger.Printf("(go %d) additional sleep after lock\n", goID)
					time.Sleep(stepSleep)
				}
				logger.Printf("(go %d) append %d<[%d] %s\n", goID, i, j, q.Query)
				gotEnds = append(gotEnds, fmt.Sprintf("%d:%d", i, j))
			}

			// err = tx.Commit()
			// if err != nil {
			// 	panic(err)
			// }
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
			// default:
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
		t.Errorf("gotStarts mismatch (-want +got):\n%s", diff)
	}

	if wantEnds != nil {
		if diff := cmp.Diff(wantEnds, gotEnds); diff != "" {
			t.Errorf("gotEnds mismatch (-want +got):\n%s", diff)
		}
	}
}
