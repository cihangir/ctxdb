package ctxdb

import (
	"database/sql"
	"testing"
	"time"

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

func TestExec(t *testing.T) {
	sql := `CREATE TABLE nullable (
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
	res, err := db.Exec(context.Background(), sql)
	if err != nil {
		t.Fatalf("err while creating table: %s", err.Error())
	}

	if res == nil {
		t.Fatalf("res should not be nil")
	}
}
