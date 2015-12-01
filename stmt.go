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
	if s.err != nil {
		return s.err
	}

	done := make(chan struct{}, 0)

	var err error
	f := func() {
		err = s.stmt.Close()
		close(done)
	}

	if opErr := s.db.handleWithGivenSQL(ctx, f, done, s.sqldb); err != nil {
		return opErr
	}

	return err
}

func (s *Stmt) Exec(ctx context.Context, args ...interface{}) (sql.Result, error) {
	if s.err != nil {
		return nil, s.err
	}

	done := make(chan struct{}, 0)

	var res sql.Result
	var err error
	f := func() {
		res, err = s.stmt.Exec(args...)
		close(done)
	}

	if opErr := s.db.handleWithGivenSQL(ctx, f, done, s.sqldb); err != nil {
		return nil, opErr
	}

	return res, err
}

func (s *Stmt) Query(ctx context.Context, args ...interface{}) (*Rows, error) {
	if s.err != nil {
		return nil, s.err
	}

	done := make(chan struct{}, 0)

	var res *sql.Rows
	var err error
	f := func() {
		res, err = s.stmt.Query(args...)
		close(done)
	}

	if opErr := s.db.handleWithGivenSQL(ctx, f, done, s.sqldb); opErr != nil {
		return nil, opErr
	}

	return &Rows{
		rows:  res,
		sqldb: s.sqldb,
		db:    s.db,
	}, nil
}

func (s *Stmt) QueryRow(ctx context.Context, args ...interface{}) *Row {
	if s.err != nil {
		return &Row{err: s.err}
	}

	done := make(chan struct{}, 0)

	var res *sql.Row
	f := func() {
		res = s.stmt.QueryRow(args...)
		close(done)
	}

	if opErr := s.db.handleWithGivenSQL(ctx, f, done, s.sqldb); opErr != nil {
		return &Row{err: opErr}
	}

	return &Row{
		row:   res,
		sqldb: s.sqldb,
		db:    s.db,
	}
}
