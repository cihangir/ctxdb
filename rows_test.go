package ctxdb

import (
	"testing"
	"time"

	"golang.org/x/net/context"
)

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

func TestScanNilChecks(t *testing.T) {
	db := getConn(t)
	ensureNullableTable(t, db)
	ctx := context.Background()

	n := &nullable{}
	row := db.QueryRow(ctx, "SELECT string_n_val FROM nullable")

	row.sqldb = nil
	if err := row.Scan(ctx, &n.StringNVal); err != errNoSqlDB {
		t.Fatalf("expected errNoSqlDB, got: %s", err)
	}

	row.db = nil
	if err := row.Scan(ctx, &n.StringNVal); err != errNoDB {
		t.Fatalf("expected errNoDB, got: %s", err)
	}

	row.row = nil
	if err := row.Scan(ctx, &n.StringNVal); err != errNoRow {
		t.Fatalf("expected errNoDB, got: %s", err)
	}
}

func TestRowsClose(t *testing.T) {
	db := getConn(t)
	ensureNullableTable(t, db)
	ctx := context.Background()

	rows, err := db.Query(ctx, "SELECT string_n_val FROM nullable")
	if err != nil {
		t.Fatalf("expected nil, got: %s", err)
	}

	if err := rows.Close(ctx); err != nil {
		t.Fatalf("expected nil, got: %s", err)
	}
}
