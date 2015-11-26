package ctxdb

import (
	"database/sql"

	"golang.org/x/net/context"
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

func (r *Row) Scan(ctx context.Context, dest ...interface{}) (err error) {
	// we can safely return here since db connections are handled on previous step
	if r.err != nil {
		return r.err
	}

	// internal validations
	if r.row == nil {
		panic("no row")
	}

	if r.db == nil {
		panic("no db")
	}

	if r.sqldb == nil {
		panic("no sqldb")
	}

	// do not forget to put back connection
	defer func() {
		select {
		case r.db.sem <- struct{}{}:
		default:
			panic("overflow 1-->")
		}
	}()

	done := make(chan struct{}, 1)

	go func() {
		r.err = r.row.Scan(dest...)
		close(done)
	}()

	select {
	case <-ctx.Done():
		// can be nil when we have timeout on db connection obtaining
		err = r.sqldb.Close()
		if err != nil {
			return err
		}

		err = ctx.Err()
		return err
	case <-done:
		err = r.db.put(r.sqldb)
		if err != nil {
			return err
		}

		err = r.err
		return err
	}

}

func (rs *Rows) Close(ctx context.Context) error {
	done := make(chan struct{}, 1)
	var err error
	f := func() {
		err = rs.rows.Close()
		close(done)
	}

	if err := rs.processRows(ctx, f, done); err != nil {
		return err
	}

	if err == nil {
		if err := rs.db.put(rs.sqldb); err != nil {
			return err
		}
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

	if err := rs.processRows(ctx, f, done); err != nil {
		return nil, err
	}

	return columns, err
}

func (rs *Rows) Err() error {
	return rs.rows.Err()
}

func (rs *Rows) Next(ctx context.Context) bool {
	done := make(chan struct{}, 1)
	var res bool
	f := func() {
		res = rs.rows.Next()
		close(done)
	}

	if err := rs.processRows(ctx, f, done); err != nil {
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

	if err := rs.processRows(ctx, f, done); err != nil {
		return err
	}

	return err
}

func (rs *Rows) processRows(ctx context.Context, f func(), done chan struct{}) error {
	go f()

	select {
	case <-ctx.Done():
		if err := rs.sqldb.Close(); err != nil {
			return err
		}

		return ctx.Err()
	case <-done:
		return nil
	}
}
