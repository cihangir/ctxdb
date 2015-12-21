package ctxdb

import (
	"database/sql"
	"errors"
	"sync"

	"golang.org/x/net/context"
)

var (
	errNoRow   = errors.New("no row")
	errNoDB    = errors.New("no db")
	errNoSQLDB = errors.New("no sqldb")
)

// Row is the result of calling QueryRow to select a single row.
type Row struct {
	row   *sql.Row
	sqldb *sql.DB
	db    *DB
	err   error
}

// Rows is the result of a query. Its cursor starts before the first row
// of the result set. Use Next to advance through the rows:
//
//     rows, err := db.Query("SELECT ...")
//     ...
//     defer rows.Close(ctx)
//     for rows.Next(ctx) {
//         var id int
//         var name string
//         err = rows.Scan(ctx, &id, &name)
//         ...
//     }
//     err = rows.Err() // get any error encountered during iteration
//     ...
type Rows struct {
	rows  *sql.Rows
	sqldb *sql.DB
	db    *DB
	err   error
	mu    sync.Mutex
}

func (r *Row) Scan(ctx context.Context, dest ...interface{}) error {
	// we can safely return here since db connections are handled on previous step
	if r.err != nil {
		return r.err
	}

	// internal validations
	if r.row == nil {
		return errNoRow
	}

	if r.db == nil {
		return errNoDB
	}

	if r.sqldb == nil {
		return errNoSQLDB
	}

	done := make(chan struct{}, 1)

	f := func() {
		r.err = r.row.Scan(dest...)
		close(done)
	}

	if err := r.db.processWithGivenSQL(ctx, f, done, r.sqldb); err != nil {
		return err
	}

	return r.err
}

func (rs *Rows) Close(ctx context.Context) error {
	if rs.err != nil {
		return rs.err
	}

	done := make(chan struct{}, 1)
	var err error
	f := func() {
		err = rs.rows.Close()
		close(done)
	}

	if err := rs.db.processWithGivenSQL(ctx, f, done, rs.sqldb); err != nil {
		return err
	}

	return err
}

func (rs *Rows) Columns(ctx context.Context) ([]string, error) {
	if rs.err != nil {
		return nil, rs.err
	}

	done := make(chan struct{}, 1)
	var err error
	var columns []string
	f := func() {
		columns, err = rs.rows.Columns()
		close(done)
	}

	if err := rs.db.handleWithGivenSQL(ctx, f, done, rs.sqldb); err != nil {
		return nil, err
	}

	return columns, err
}

func (rs *Rows) Err() error {
	if rs.err != nil {
		return rs.err
	}

	return rs.rows.Err()
}

func (rs *Rows) Next(ctx context.Context) bool {
	if rs.err != nil {
		return false
	}

	done := make(chan struct{}, 1)
	var res bool
	f := func() {
		res = rs.rows.Next()
		close(done)
	}

	if err := rs.db.handleWithGivenSQL(ctx, f, done, rs.sqldb); err != nil {
		rs.err = err
		return false
	}

	return res
}

func (rs *Rows) Scan(ctx context.Context, dest ...interface{}) error {
	if rs.err != nil {
		return rs.err
	}

	done := make(chan struct{}, 1)
	var err error
	f := func() {
		err = rs.rows.Scan(dest...)
		close(done)
	}

	return rs.db.handleWithGivenSQL(ctx, f, done, rs.sqldb)
}
