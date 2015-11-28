package ctxdb

import (
	"database/sql"
	"testing"
	"time"

	"github.com/cihangir/nisql"
	_ "github.com/lib/pq"
	"golang.org/x/net/context"
)

func TestPing(t *testing.T) {
	p := getConn(t)

	ctx := context.Background()
	if err := p.Ping(ctx); err != nil {
		t.Errorf("Err while pinging: %# v", err)
	}

	if err := p.Close(); err != nil {
		t.Errorf("Err should be nil:  got %# v", err)
	}
}

func TestProcess(t *testing.T) {
	p := getConn(t)

	// test global time-out
	timedoutCtx, cancel1 := context.WithTimeout(
		context.Background(),
		time.Millisecond,
	)
	defer cancel1() // releases resources if slowOperation completes before timeout elapses

	done1 := make(chan struct{}, 1)
	f := func(sqldb *sql.DB) {
		time.Sleep(time.Millisecond * 100)
		close(done1)
	}

	time.Sleep(time.Millisecond * 2)
	if err := p.process(timedoutCtx, f, done1); err != context.DeadlineExceeded {
		t.Errorf("Expected deadline exceeded, got: %# v", err)
	}

	done2 := make(chan struct{}, 1)
	f = func(sqldb *sql.DB) {
		time.Sleep(time.Millisecond * 120)
		close(done2)
	}

	// test sem aquired timeout
	semtimeoutCtx, cancel2 := context.WithTimeout(
		context.Background(),
		time.Millisecond*100,
	)
	defer cancel2()

	if err := p.process(semtimeoutCtx, f, done2); err != context.DeadlineExceeded {
		t.Errorf("Expected deadline exceeded, got: %# v", err)
	}

	done3 := make(chan struct{}, 1)
	f = func(sqldb *sql.DB) {
		time.Sleep(time.Millisecond * 120)
		close(done3)
	}
	semtimeoutCtx3, cancel3 := context.WithTimeout(
		context.Background(),
		time.Millisecond*100,
	)
	defer cancel3()
	p.conns = nil
	if err := p.process(semtimeoutCtx3, f, done2); err != ErrClosed {
		t.Errorf("Expected ClosedConnection, got: %# v", err)
	}
}

type nullable struct {
	StringNVal nisql.NullString
	StringVal  string

	Int64NVal nisql.NullInt64
	Int64Val  int64

	Float64NVal nisql.NullFloat64
	Float64Val  float64

	BoolNVal nisql.NullBool
	BoolVal  bool

	TimeNVal nisql.NullTime
	TimeVal  time.Time
}

func TestExec(t *testing.T) {
	db := getConn(t)
	ensureNullableTable(t, db) // uses Exec internally
}

func ensureNullableTable(t *testing.T, db *DB) {

	ctx := context.Background()
	res, err := db.Exec(ctx, createTableSqlStatement)
	if err != nil {
		t.Fatalf("Error while ensuring the nullable table %+v", err)
	}

	if res == nil {
		t.Fatalf("res should not be nil")
	}

}
func TestExecWithTimeout(t *testing.T) {
	db := getConn(t)
	ensureNullableTable(t, db) // uses Exec internally

	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Nanosecond*10)
	defer cancel()

	_, err := db.Exec(ctx, insertSqlStatement, 42, nil, 12)
	if err != context.DeadlineExceeded {
		t.Fatalf("expected context.DeadlineExceeded, got: %s", err)
	}

	sqlStatement := `SELECT * FROM nullable WHERE
    int64_val = $1 AND
    float64_n_val = $2 AND
    float64_val = $3
    `

	ctx = context.Background() // reset

	n := &nullable{}
	err = db.QueryRow(ctx, sqlStatement, 42, nil, 12).
		Scan(ctx, &n.StringNVal,
		&n.StringVal,
		&n.Int64NVal,
		&n.Int64Val,
		&n.Float64NVal,
		&n.Float64Val,
		&n.BoolNVal,
		&n.BoolVal,
		&n.TimeNVal,
		&n.TimeVal,
	)
	if err != sql.ErrNoRows {
		t.Fatalf("expected sql.ErrNoRows, got %s", err)
	}
}

func TestQueryRow(t *testing.T) {
	db := getConn(t)
	ensureNullableTable(t, db)
	ctx := context.Background()

	if _, err := db.Exec(ctx, insertSqlStatement, 42, nil, 12); err != nil {
		t.Fatalf("err while adding null item: %s", err.Error())
	}

	n := &nullable{}
	err := db.QueryRow(ctx, "SELECT * FROM nullable").
		Scan(ctx, &n.StringNVal,
		&n.StringVal,
		&n.Int64NVal,
		&n.Int64Val,
		&n.Float64NVal,
		&n.Float64Val,
		&n.BoolNVal,
		&n.BoolVal,
		&n.TimeNVal,
		&n.TimeVal,
	)
	if err != nil {
		t.Fatalf(err.Error())
	}

	if n.StringVal != "NULLABLE" {
		t.Fatalf("expected NULLABLE, got: ", n.StringVal)
	}

	if n.StringNVal.Valid {
		t.Fatalf("expected invalid, got valid for string_n_val")
	}

	if n.Int64Val != int64(42) {
		t.Fatalf("expected 42, got: %d", n.Int64Val)
	}

	if n.Int64NVal.Valid {
		t.Fatalf("expected invalid, got valid for int64_n_val")
	}

	if n.Float64Val != float64(12) {
		t.Fatalf("expected 12, got: %f", n.Float64Val)
	}

	if n.Float64NVal.Valid {
		t.Fatalf("expected invalid, got valid for float64_n_val")
	}

	if n.BoolVal != true {
		t.Fatalf("expected true, got: %t", n.BoolVal)
	}

	if n.BoolNVal.Valid {
		t.Fatalf("expected invalid, got valid for bool_n_val")
	}

	if n.TimeNVal.Valid {
		t.Fatalf("expected false, got: %t", n.TimeNVal)
	}

	if n.TimeVal.IsZero() {
		t.Fatalf("expected valid, got invalid for TimeVal: %+v", n.TimeVal)
	}

	if _, err := db.Exec(ctx, "DELETE FROM nullable"); err != nil {
		t.Fatalf("err while clearing nullable table: %s", err.Error())
	}

	if _, err := db.Exec(ctx, deleteSqlStatement); err != nil {
		t.Fatalf("err while cleaning the database: %s", err.Error())
	}
}

func TestQueryRowWithPoolFailure(t *testing.T) {
	db := getConn(t)
	ensureNullableTable(t, db)
	ctx := context.Background()

	db.conns = nil
	row := db.QueryRow(ctx, "SELECT string_n_val FROM nullable")
	if row.err != ErrClosed {
		t.Fatalf("expected ErrClosed got: %+v", row)
	}
}

func TestQueryRowWithTimeout(t *testing.T) {
	db := getConn(t)
	ensureNullableTable(t, db)
	ctx := context.Background()

	if _, err := db.Exec(ctx, insertSqlStatement, 42, nil, 12); err != nil {
		t.Fatalf("err while adding null item: %s", err.Error())
	}

	timeoutDuration := time.Millisecond * 100 // sleep for

	//
	// test queryrow with timedoutCxt
	//
	timedoutCtx1, cancel1 := context.WithTimeout(ctx, timeoutDuration)
	defer cancel1()

	time.Sleep(timeoutDuration * 2)
	n := &nullable{}
	row := db.QueryRow(timedoutCtx1, "SELECT string_n_val FROM nullable")
	err := row.Scan(ctx, &n.StringNVal)
	if err != context.DeadlineExceeded {
		t.Fatalf("expected context.DeadlineExceeded, got: %s", err)
	}

	//
	// test scan & queryrow with timedoutCxt
	//
	timedoutCtx2, cancel2 := context.WithTimeout(ctx, timeoutDuration)
	defer cancel2()
	time.Sleep(timeoutDuration * 2)

	row = db.QueryRow(timedoutCtx2, "SELECT string_n_val FROM nullable")
	err = row.Scan(timedoutCtx2, &n.StringNVal)
	if err != context.DeadlineExceeded {
		t.Fatalf("expected context.DeadlineExceeded, got: %s", err)
	}

	if _, err := db.Exec(ctx, deleteSqlStatement); err != nil {
		t.Fatalf("err while cleaning the database: %s", err.Error())
	}

	timedoutCtx3, cancel3 := context.WithTimeout(ctx, timeoutDuration)
	defer cancel3()
	time.Sleep(timeoutDuration * 2)

	row = db.QueryRow(timedoutCtx3, "SELECT string_n_val FROM nullable")
	err = row.Scan(timedoutCtx3, &n.StringNVal)
	if err != context.DeadlineExceeded {
		t.Fatalf("expected context.DeadlineExceeded, got: %s", err)
	}

	if _, err := db.Exec(ctx, deleteSqlStatement); err != nil {
		t.Fatalf("err while cleaning the database: %s", err.Error())
	}

	timedoutCtx4, cancel4 := context.WithTimeout(ctx, time.Millisecond)
	defer cancel4()
	time.Sleep(time.Millisecond)

	// consume all the sems to drop to ctx.Done immediately&reliably
	for i := 0; i < cap(db.sem); i++ {
		select {
		case <-db.sem:
		default:
		}
	}

	row = db.QueryRow(timedoutCtx4, "SELECT string_n_val FROM nullable")
	if row.err != context.DeadlineExceeded {
		t.Fatalf("expected context.DeadlineExceeded, got: %+v", row)
	}
}

func TestScanWithTimeout(t *testing.T) {
	db := getConn(t)
	ensureNullableTable(t, db)
	ctx := context.Background()

	if _, err := db.Exec(ctx, insertSqlStatement, 42, nil, 12); err != nil {
		t.Fatalf("err while adding null item: %s", err.Error())
	}

	timeoutDuration := time.Millisecond
	timedoutCtx, cancel := context.WithTimeout(ctx, timeoutDuration)
	defer cancel()

	n := &nullable{}
	row := db.QueryRow(ctx, "SELECT string_n_val FROM nullable")
	time.Sleep(timeoutDuration)
	err := row.Scan(timedoutCtx, &n.StringNVal)
	if err != context.DeadlineExceeded {
		t.Fatalf("expected context.DeadlineExceeded, got: %s", err)
	}

	if _, err := db.Exec(ctx, deleteSqlStatement); err != nil {
		t.Fatalf("err while cleaning the database: %s", err.Error())
	}
}

var (
	insertSqlStatement = `INSERT INTO nullable
VALUES ( NULL, 'NULLABLE', NULL, $1, $2, $3, NULL, true, NULL, NOW() )`

	createTableSqlStatement = `CREATE TABLE IF NOT EXISTS nullable (
    string_n_val VARCHAR (255) DEFAULT NULL,
    string_val VARCHAR (255) DEFAULT 'empty',
    int64_n_val BIGINT DEFAULT NULL,
    int64_val BIGINT DEFAULT 1,
    float64_n_val NUMERIC DEFAULT NULL,
    float64_val NUMERIC DEFAULT 1,
    bool_n_val BOOLEAN,
    bool_val BOOLEAN NOT NULL,
    time_n_val timestamp,
    time_val timestamp NOT NULL
)`
	deleteSqlStatement = `DELETE FROM nullable`
)
