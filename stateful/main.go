package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync/atomic"
	"time"

	"github.com/acoshift/pgsql"
	"github.com/acoshift/pgsql/pgctx"
	"github.com/acoshift/pgsql/pgstmt"
	"github.com/google/uuid"
	"github.com/lib/pq"
)

// benchmark parameter
const (
	// benchmark time
	d = 5 * time.Second

	// number of users
	n = 4400

	// number of concurrent per user
	conPerUser = 200
)

func main() {
	dbURL := os.Getenv("DB_URL")
	if dbURL == "" {
		dbURL = "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"
	}
	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		log.Fatalf("can not open db: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(30)

	// migrate
	_, err = db.Exec(`
		create table if not exists user_points (
		    user_id varchar,
		    balance bigint not null,
		    primary key (user_id)
		);
		create table if not exists point_txs (
		    id uuid,
		    user_id varchar not null,
		    amount bigint not null,
		    created_at timestamptz not null default now(),
		    primary key (id)
		);
		truncate table user_points;
		truncate table point_txs;
	`)
	if err != nil {
		log.Fatalf("can not migrate: %v", err)
	}

	uuid.EnableRandPool()

	ctx := context.Background()
	ctx = pgctx.NewContext(ctx, db)

	{
		fmt.Println("Running stateless load test...")

		nctx, _ := context.WithTimeout(ctx, d)
		start := time.Now()
		for i := 0; i < n; i++ {
			go newLoadWorkerStateless(nctx)
		}
		<-nctx.Done()
		printBenchResult(start)
	}

	time.Sleep(time.Second)

	_, err = db.Exec(`
		truncate table user_points;
		truncate table point_txs;
	`)
	if err != nil {
		log.Fatalf("can not truncate: %v", err)
	}

	opCnt = 0
	errCnt = 0

	{
		fmt.Println("Running stateful load test...")

		go startBgWorker(ctx)

		nctx, _ := context.WithTimeout(ctx, d)

		start := time.Now()
		for i := 0; i < n; i++ {
			go newLoadWorkerStateful(nctx)
		}
		<-nctx.Done()
		printBenchResult(start)
	}
}

func printBenchResult(start time.Time) {
	diff := time.Since(start)
	cnt := atomic.LoadUint64(&opCnt)
	err := atomic.LoadUint64(&errCnt)
	fmt.Printf("duration: %s\n", diff)
	fmt.Printf("operations: %d\n", cnt)
	fmt.Printf("errors: %d\n", err)
	fmt.Printf("op/s: %d\n", (cnt+err)/uint64(diff/time.Second))
}

func addPoint(ctx context.Context, userID string, amount int64) error {
	return pgctx.RunInTx(ctx, func(ctx context.Context) error {
		var balance int64
		err := pgctx.QueryRow(ctx, `
			select balance
			from user_points
			where user_id = $1
		`, userID).Scan(&balance)
		if errors.Is(err, sql.ErrNoRows) {
			err = nil
		}
		if err != nil {
			return err
		}

		balance += amount
		if balance < 0 {
			return errors.New("insufficient balance")
		}

		_, err = pgctx.Exec(ctx, `
			insert into user_points (user_id, balance)
			values ($1, $2)
			on conflict (user_id) do update
			set balance = $2
		`, userID, balance)
		if err != nil {
			return err
		}

		_, err = pgctx.Exec(ctx, `
			insert into point_txs (id, user_id, amount)
			values ($1, $2, $3)
		`, uuid.NewString(), userID, amount)
		if err != nil {
			return err
		}

		return nil
	})
}

var (
	opCnt  uint64
	errCnt uint64
)

func newLoadWorkerStateless(ctx context.Context) {
	userID := uuid.NewString()

	for i := 0; i < conPerUser; i++ {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				err := addPoint(ctx, userID, rand.Int63n(100))
				if errors.Is(err, context.DeadlineExceeded) {
					return
				}
				if err != nil {
					atomic.AddUint64(&errCnt, 1)
					continue
				}
				atomic.AddUint64(&opCnt, 1)
			}
		}()
	}
}

type callback struct {
	err error
}

type op struct {
	userID string
	amount int64
	done   chan<- callback
}

type txLog struct {
	txID   string
	userID string
	amount int64
}

var opChan = make(chan op, 20000)

func startBgWorker(ctx context.Context) {
	const buffSize = 7000
	buff := make([]op, 0, buffSize)
	callbacks := make([]callback, 0, buffSize)
	txLogs := make([]txLog, 0, buffSize)

	restoreState := func(keys []string) (map[string]int64, error) {
		m := map[string]int64{}
		if len(keys) == 0 {
			return m, nil
		}

		err := pgctx.Iter(ctx, func(scan pgsql.Scanner) error {
			var (
				userID  string
				balance int64
			)
			err := scan(&userID, &balance)
			if err != nil {
				return err
			}
			m[userID] = balance
			return nil
		}, `
			select user_id, balance
			from user_points
			where user_id = any($1)
		`, pq.Array(keys))
		if err != nil {
			return nil, err
		}
		return m, nil
	}

	batchInsertTxLogs := func() error {
		if len(txLogs) == 0 {
			return nil
		}

		_, err := pgstmt.Insert(func(b pgstmt.InsertStatement) {
			b.Into("point_txs")
			b.Columns("id", "user_id", "amount")
			for _, tx := range txLogs {
				b.Value(tx.txID, tx.userID, tx.amount)
			}
		}).ExecWith(ctx)
		return err
	}

	saveDirtyState := func(state map[string]int64, dirty map[string]struct{}) error {
		if len(dirty) == 0 {
			return nil
		}

		_, err := pgstmt.Insert(func(b pgstmt.InsertStatement) {
			b.Into("user_points")
			b.Columns("user_id", "balance")
			for userID := range dirty {
				b.Value(userID, state[userID])
			}
			b.OnConflict("user_id").DoUpdate(func(b pgstmt.UpdateStatement) {
				b.Set("balance").ToRaw("excluded.balance")
			})
		}).ExecWith(ctx)
		return err
	}

	flush := func() {
		if len(buff) == 0 {
			return
		}

		restoreUserIDs := make([]string, 0, len(buff))
		for _, p := range buff {
			restoreUserIDs = append(restoreUserIDs, p.userID)
		}

		err := pgctx.RunInTx(ctx, func(ctx context.Context) error {
			dirty := map[string]struct{}{}

			state, err := restoreState(restoreUserIDs)
			if err != nil {
				return err
			}

			txLogs = txLogs[:0]
			callbacks = callbacks[:0]

			for _, p := range buff {
				balance := state[p.userID]
				balance += p.amount

				var cb callback
				if balance < 0 {
					cb.err = errors.New("insufficient balance")
					callbacks = append(callbacks, cb)
					continue
				}

				state[p.userID] = balance
				dirty[p.userID] = struct{}{}
				txLogs = append(txLogs, txLog{
					txID:   uuid.NewString(),
					userID: p.userID,
					amount: p.amount,
				})
				callbacks = append(callbacks, cb)
			}

			err = batchInsertTxLogs()
			if err != nil {
				return err
			}

			err = saveDirtyState(state, dirty)
			if err != nil {
				return err
			}

			return nil
		})
		if err != nil {
			log.Printf("flush error: %v", err)
			return
		}

		for i, p := range buff {
			p.done <- callbacks[i]
		}
		buff = buff[:0]
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(100 * time.Millisecond):
			flush()
		case p := <-opChan:
			buff = append(buff, p)
			if len(buff) >= buffSize {
				flush()
			}
		}
	}
}

func addPointStateful(userID string, amount int64) error {
	done := make(chan callback, 1)
	opChan <- op{userID: userID, amount: amount, done: done}
	cb := <-done
	return cb.err
}

func newLoadWorkerStateful(ctx context.Context) {
	userID := uuid.NewString()

	for i := 0; i < conPerUser; i++ {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				err := addPointStateful(userID, rand.Int63n(100))
				if errors.Is(err, context.DeadlineExceeded) {
					return
				}
				if err != nil {
					atomic.AddUint64(&errCnt, 1)
					continue
				}
				atomic.AddUint64(&opCnt, 1)
			}
		}()
	}
}
