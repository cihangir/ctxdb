package ctxdb

import (
	"database/sql"

	"golang.org/x/net/context"
)

type Stmt struct {
	stmt  *sql.Stmt
	query string
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
	f := func(sqldb *sql.DB) {
		defer close(done)

		var stmt *sql.Stmt
		stmt, err = sqldb.Prepare(s.query)
		if err != nil {
			return
		}

		res, err = stmt.Exec(args...)

	}

	if opErr := s.db.process(ctx, f, done); opErr != nil {
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

	f := func(sqldb *sql.DB) {
		defer close(done)

		var stmt *sql.Stmt
		stmt, err = sqldb.Prepare(s.query)
		if err != nil {
			return
		}

		res, err = stmt.Query(args...)

	}

	sqldb, opErr := s.db.handleWithSQL(ctx, f, done)
	if opErr != nil {
		return nil, opErr
	}

	if err != nil {
		return nil, err
	}

	return &Rows{
		rows:  res,
		sqldb: sqldb,
		db:    s.db,
	}, nil
}

func (s *Stmt) QueryRow(ctx context.Context, args ...interface{}) *Row {
	if s.err != nil {
		return &Row{err: s.err}
	}

	done := make(chan struct{}, 0)

	var res *sql.Row
	f := func(sqldb *sql.DB) {
		defer close(done)

		var stmt *sql.Stmt
		stmt, err := sqldb.Prepare(s.query)
		if err != nil {
			return
		}

		res = stmt.QueryRow(args...)
	}

	if _, opErr := s.db.handleWithSQL(ctx, f, done); opErr != nil {
		return &Row{err: opErr}
	}

	return &Row{
		row:   res,
		sqldb: s.sqldb,
		db:    s.db,
	}
}
