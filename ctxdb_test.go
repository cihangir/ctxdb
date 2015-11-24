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

	done := make(chan struct{}, 1)
	f := func(sqldb *sql.DB) {
		time.Sleep(time.Millisecond * 200)
		close(done)
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		time.Millisecond*100,
	)
	defer cancel() // releases resources if slowOperation completes before timeout elapses

	if err := p.process(ctx, f, done); err != context.DeadlineExceeded {
		t.Errorf("Expected deadline exceeded, got: %# v", err)
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
	sqlStatement := `CREATE TABLE nullable (
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

	db := getConn(t)
	ctx := context.Background()
	res, err := db.Exec(ctx, sqlStatement)
	if err != nil {
		t.Fatalf("err while creating table: %s", err.Error())
	}

	if res == nil {
		t.Fatalf("res should not be nil")
	}

	sqlStatement = `INSERT INTO nullable
VALUES
    (
        NULL,
        'NULLABLE',
        NULL,
        $1,
        $2,
        $3,
        NULL,
        true,
        NULL,
        NOW()
    )`

	if _, err := db.Exec(ctx, sqlStatement, 42, nil, 12); err != nil {
		t.Fatalf("err while adding null item: %s", err.Error())
	}

	n := &nullable{}
	err = db.QueryRow(ctx, "SELECT * FROM nullable").
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

	// ctx, _ = context.WithTimeout(ctx, time.Nanosecond)
	// defer cancel()
	n = &nullable{}
	err = db.QueryRow(ctx, "SELECT * FROM nullable").
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
		t.Fatalf("expecting ErrNoRows")
	}

}
