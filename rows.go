package ctxdb

import (
	"database/sql"
	"errors"

	"golang.org/x/net/context"
)

var (
	errNoRow   = errors.New("no row")
	errNoDB    = errors.New("no db")
	errNoSqlDB = errors.New("no sqldb")
)

type Row struct {
	row   *sql.Row
	sqldb *sql.DB
	db    *DB
	err   error
}

type Rows struct {
	rows  *sql.Rows
	sqldb *sql.DB
	db    *DB
	err   error
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
		return errNoSqlDB
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
	done := make(chan struct{}, 1)
	var err error
	f := func() {
		err = rs.rows.Scan(dest...)
		close(done)
	}

	return rs.db.handleWithGivenSQL(ctx, f, done, rs.sqldb)
}
