package ctxdb

import (
	"database/sql"

	"golang.org/x/net/context"
)

type Stmt struct {
	stmt  *sql.Stmt
	err   error
	sqldb *sql.DB
	db    *DB
}

func (s *Stmt) Close(ctx context.Context) error {

	done := make(chan struct{}, 0)

	var err error
	go func() {
		err = s.stmt.Close()
		close(done)
	}()

	select {
	case <-ctx.Done():
		if err := s.sqldb.Close(); err != nil {
			return err
		}

		return ctx.Err()
	case <-done:
		return err
	}

}

func (s *Stmt) Exec(ctx context.Context, args ...interface{}) (sql.Result, error) {
	done := make(chan struct{}, 0)

	var res sql.Result
	var err error
	go func() {
		res, err = s.stmt.Exec(args...)
		close(done)
	}()

	select {
	case <-ctx.Done():
		if err := s.sqldb.Close(); err != nil {
			return nil, err
		}

		return nil, ctx.Err()
	case <-done:
		return res, err
	}
}

func (s *Stmt) Query(ctx context.Context, args ...interface{}) (*Rows, error) {
	done := make(chan struct{}, 0)

	var res *sql.Rows
	var err error
	go func() {
		res, err = s.stmt.Query(args...)
		close(done)
	}()

	select {
	case <-ctx.Done():
		if err := s.sqldb.Close(); err != nil {
			return nil, err
		}

		return nil, ctx.Err()
	case <-done:
		r := &Rows{
			rows:  res,
			err:   err,
			sqldb: s.sqldb,
			db:    s.db,
		}
		return r, err
	}
}

func (s *Stmt) QueryRow(ctx context.Context, args ...interface{}) *Row {
	done := make(chan struct{}, 0)

	var res *sql.Row
	go func() {
		res = s.stmt.QueryRow(args...)
		close(done)
	}()

	select {
	case <-ctx.Done():
		r := &Row{
			row:   res,
			err:   ctx.Err(),
			sqldb: s.sqldb,
			db:    s.db,
		}

		if err := s.sqldb.Close(); err != nil {
			r.err = err
		}

		return r
	case <-done:
		return &Row{
			row:   res,
			sqldb: s.sqldb,
			db:    s.db,
		}
	}
}
