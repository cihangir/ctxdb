package ctxdb

import (
	"testing"

	"golang.org/x/net/context"
)

func TestStmt(t *testing.T) {
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
}
