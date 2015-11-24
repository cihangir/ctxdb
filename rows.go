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

func (r *Row) Scan(ctx context.Context, dest ...interface{}) error {
	done := make(chan struct{}, 1)

	var err error
	go func() {
		err = r.row.Scan(dest...)
		close(done)
	}()

	// do not forget to put back
	defer func() {
		r.db.sem <- struct{}{}
	}()

	select {
	case <-ctx.Done():
		// can be nil when we have timeout on db connection obtaining
		if r.sqldb != nil {
			if err := r.sqldb.Close(); err != nil {
				return err
			}
		}

		return ctx.Err()

	case <-done:
		if err := r.db.put(r.sqldb); err != nil {
			return err
		}
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
