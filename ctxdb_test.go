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

func TestGet(t *testing.T) {
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

	if err := p.get(ctx, f, done); err != context.DeadlineExceeded {
		t.Errorf("Expected deadline exceeded, got: %# v", err)
	}
}
