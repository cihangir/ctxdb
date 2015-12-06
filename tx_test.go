package ctxdb

import (
	"errors"
	"testing"
	"time"

	"golang.org/x/net/context"
)

func TestTx(t *testing.T) {
	db := getConn(t)
	ensureNullableTable(t, db)
	ctx := context.Background()

	if _, err := db.Exec(ctx, deleteSqlStatement); err != nil {
		t.Fatalf("err while cleaning the database: %s", err.Error())
	}

	// prepare data set
	for i := 1; i < 5; i++ {
		if _, err := db.Exec(ctx, insertSqlStatement, i, nil, 42); err != nil {
			t.Fatalf("err while adding null item: %s", err.Error())
		}
	}

	query := "SELECT int64_val FROM nullable WHERE int64_val > $1 ORDER BY int64_val"
	params := []interface{}{0}

	stmt, err := db.Prepare(ctx, query)
	if err != nil {
		t.Fatalf("Err while preparing: %# v", err)
	}

	if stmt == nil {
		t.Fatalf("stmt should not be nil")
	}

	res, err := stmt.Exec(ctx, params...)
	if err != nil {
		t.Fatalf("Err while execing: %# v", err)
	}

	if res == nil {
		t.Fatalf("sql.Result should not be nil")
	}

	rows, err := stmt.Query(ctx, params...)
	if err != nil {
		t.Fatalf("Err while execing: %# v", err)
	}

	if rows == nil {
		t.Fatalf("Rows should not be nil")
	}

	var i int64 = 1
	for rows.Next(ctx) {
		var int64_val int64
		if err := rows.Scan(ctx, &int64_val); err != nil {
			t.Fatalf("expected nil, got: %s", err)
		}

		if int64_val != i {
			t.Fatalf("expected int64_val to be same with %d, got: %d", i, int64_val)
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

	row := stmt.QueryRow(ctx, params...)
	if row == nil {
		t.Fatalf("Row should not be nil")
	}

	var int64_val int64
	if err := row.Scan(ctx, &int64_val); err != nil {
		t.Fatalf("should fail", err)
	}

	// we scanned just the first row
	if int64_val != 1 {
		t.Fatalf(" int64_val should be 1, got: %d", int64_val)
	}

	if err := stmt.Close(ctx); err != nil {
		t.Fatalf("err while closing stmt %s", err)
	}

	if err := stmt.Close(ctx); err != nil {
		t.Fatalf("err while closing stmt again %s", err)
	}

}

func TestTxSimpleBeginCommit(t *testing.T) {
	db := getConn(t)
	ensureNullableTable(t, db)
	ctx := context.Background()

	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("err while beginning the transaction: %s", err)
	}

	if tx == nil {
		t.Fatalf("tx should not be nil: %s", err)
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("err while committing the tx: err : %s", err)
	}
}

func TestTxBeginCommitWithTimeout(t *testing.T) {
	db := getConn(t)
	ensureNullableTable(t, db)
	ctx := context.Background()

	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("err while beginning the transaction: %s", err)
	}
	if tx == nil {
		t.Fatalf("tx should not be nil: %s", err)
	}

	timeout := time.Millisecond * 10
	ctx2, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	time.Sleep(timeout * 2)
	if err := tx.Commit(ctx2); err != context.DeadlineExceeded {
		t.Fatalf("err should be context.DeadlineExceeded while committing the tx: err : %s", err)
	}
}

func TestTxBeginWithTimeout(t *testing.T) {
	db := getConn(t)
	ensureNullableTable(t, db)
	ctx := context.Background()
	timeout := time.Millisecond * 10
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	time.Sleep(timeout * 2)

	tx, err := db.Begin(ctx)
	if err != context.DeadlineExceeded {
		t.Fatalf("err should be context.DeadlineExceeded while committing the tx: err : %s", err)
	}

	if tx != nil {
		t.Fatalf("tx should be nil")
	}
}

func TestTxSimpleBeginRollback(t *testing.T) {
	db := getConn(t)
	ensureNullableTable(t, db)
	ctx := context.Background()

	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("err while beginning the transaction: %s", err)
	}

	if tx == nil {
		t.Fatalf("tx should not be nil: %s", err)
	}

	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("err while rolling back the tx: err : %s", err)
	}
}

func TestTxBeginRollbackWithTimeout(t *testing.T) {
	db := getConn(t)
	ensureNullableTable(t, db)
	ctx := context.Background()

	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("err while beginning the transaction: %s", err)
	}

	if tx == nil {
		t.Fatalf("tx should not be nil: %s", err)
	}

	timeout := time.Millisecond * 10
	ctx2, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	time.Sleep(timeout * 2)
	if err := tx.Rollback(ctx2); err != context.DeadlineExceeded {
		t.Fatalf("err should be context.DeadlineExceeded while committing the tx: err : %s", err)
	}
}

func TestTxCommitWithStickyError(t *testing.T) {
	db := getConn(t)
	ensureNullableTable(t, db)
	ctx := context.Background()
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("err while beginning the transaction: %s", err)
	}

	if tx == nil {
		t.Fatalf("tx should not be nil")
	}

	stickyErr := errors.New("stickyErr")
	tx.stickyErr = stickyErr
	if err := tx.Commit(ctx); err != stickyErr {
		t.Fatalf("err should be  stickyErr while rolling back the tx: got err : %s", err)
	}
}

func TestTxRollbackWithStickyError(t *testing.T) {
	db := getConn(t)
	ensureNullableTable(t, db)
	ctx := context.Background()
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("err while beginning the transaction: %s", err)
	}

	if tx == nil {
		t.Fatalf("tx should not be nil")
	}

	stickyErr := errors.New("stickyErr")
	tx.stickyErr = stickyErr
	if err := tx.Rollback(ctx); err != stickyErr {
		t.Fatalf("err should be  stickyErr while rolling back the tx: got err : %s", err)
	}
}

func TestTxExecWithStickyError(t *testing.T) {
	db := getConn(t)
	ensureNullableTable(t, db)
	ctx := context.Background()
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("err while beginning the transaction: %s", err)
	}

	if tx == nil {
		t.Fatalf("tx should not be nil")
	}

	query := "SELECT pg_sleep($1);"
	param := "1"

	stickyErr := errors.New("stickyErr")
	tx.stickyErr = stickyErr
	if _, err := tx.Exec(ctx, query, param); err != stickyErr {
		t.Fatalf("err should be  stickyErr while rolling back the tx: got err : %s", err)
	}
}
