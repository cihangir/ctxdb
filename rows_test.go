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

	if _, err := db.Exec(ctx, insertSQLStatement, 42, nil, 12); err != nil {
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

	if _, err := db.Exec(ctx, deleteSQLStatement); err != nil {
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
	if err := row.Scan(ctx, &n.StringNVal); err != errNoSQLDB {
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

func TestRowsColumns(t *testing.T) {
	db := getConn(t)
	ensureNullableTable(t, db)
	ctx := context.Background()

	if _, err := db.Exec(ctx, insertSQLStatement, 42, nil, 12); err != nil {
		t.Fatalf("err while adding null item: %s", err.Error())
	}

	rows, err := db.Query(ctx, "SELECT string_n_val FROM nullable")
	if err != nil {
		t.Fatalf("expected nil, got: %s", err)
	}

	columns, err := rows.Columns(ctx)
	if err != nil {
		t.Fatalf("expected nil, got: %s", err)
	}

	if len(columns) != 1 {
		t.Fatalf("expected 1 column, got: %d", len(columns))
	}

	if columns[0] != "string_n_val" {
		t.Fatalf("expected string_n_val column, got: %d", columns[0])
	}
}

func TestRowsColumnsWithTimeout(t *testing.T) {
	db := getConn(t)
	ensureNullableTable(t, db)
	ctx := context.Background()

	if _, err := db.Exec(ctx, insertSQLStatement, 42, nil, 12); err != nil {
		t.Fatalf("err while adding null item: %s", err.Error())
	}

	timeout := time.Millisecond * 50
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	rows, err := db.Query(ctx, "SELECT string_n_val FROM nullable")
	if err != nil {
		t.Fatalf("expected nil, got: %s", err)
	}

	time.Sleep(timeout * 2)

	columns, err := rows.Columns(ctx)
	if err != context.DeadlineExceeded {
		t.Fatalf("expected context.DeadlineExceeded, got: %s", err)
	}

	if len(columns) != 0 {
		t.Fatalf("expected 0 column, got: %d", len(columns))
	}
}

func TestRowsScanNextErr(t *testing.T) {
	db := getConn(t)
	ensureNullableTable(t, db)
	ctx := context.Background()

	if _, err := db.Exec(ctx, deleteSQLStatement); err != nil {
		t.Fatalf("err while cleaning the database: %s", err.Error())
	}

	// prepare data set
	for i := 1; i < 5; i++ {
		if _, err := db.Exec(ctx, insertSQLStatement, i, nil, 42); err != nil {
			t.Fatalf("err while adding null item: %s", err.Error())
		}
	}

	rows, err := db.Query(ctx, "SELECT int64_val FROM nullable WHERE int64_val > $1 ORDER BY int64_val", 0)
	if err != nil {
		t.Fatalf("expected nil, got: %s", err)
	}

	var i int64 = 1
	for rows.Next(ctx) {
		var int64val int64
		if err := rows.Scan(ctx, &int64val); err != nil {
			t.Fatalf("expected nil, got: %s", err)
		}

		if int64val != i {
			t.Fatalf("expected int64val to be same with %d, got: %d", i, int64val)
		}

		i++
	}

	if i != 5 {
		t.Fatalf("i should be 5, got: %d", i)
	}

	if err := rows.Err(); err != nil {
		t.Fatalf("expected nil, got: %s", err)
	}

	if err := rows.Close(ctx); err != nil {
		t.Fatalf("expected nil, got: %s", err)
	}
}

func TestRowsScanWithNoResult(t *testing.T) {
	db := getConn(t)
	ensureNullableTable(t, db)
	ctx := context.Background()

	rows, err := db.Query(ctx, "SELECT string_n_val FROM nullable WHERE int64_n_val <> int64_n_val")
	if err != nil {
		t.Fatalf("expected nil, got: %s", err)
	}

	for rows.Next(ctx) {
		t.Fatalf("expected no result, but got")
	}

	if err := rows.Err(); err != nil {
		t.Fatalf("expected nil, got: %s", err)
	}

	if err := rows.Close(ctx); err != nil {
		t.Fatalf("expected nil, got: %s", err)
	}
}

func TestRowsNextWithTimeout(t *testing.T) {
	db := getConn(t)
	ensureNullableTable(t, db)
	ctx := context.Background()

	rows, err := db.Query(ctx, "SELECT string_n_val FROM nullable WHERE int64_n_val > $1 ORDER BY int64_n_val", 0)
	if err != nil {
		t.Fatalf("expected nil, got: %s", err)
	}

	timeout := time.Millisecond * 50
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	time.Sleep(timeout * 2)
	for rows.Next(ctx) {
		t.Fatalf("expected no result, but got")
	}

	if err := rows.Err(); err != context.DeadlineExceeded {
		t.Fatalf("expected context.DeadlineExceeded, got: %s", err)
	}

	if err := rows.Close(ctx); err != context.DeadlineExceeded {
		t.Fatalf("expected context.DeadlineExceeded, got: %s", err)
	}
}
