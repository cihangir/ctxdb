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

// Exec executes a prepared statement with the given arguments and returns a Result
// summarizing the effect of the statement.
//
// Exec prepares the same statement on another connection and executes it
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

// Query executes a prepared query statement with the given arguments and
// returns the query results as a *Rows.
//
// Query prepares the same statement on another connection and queries it
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

// QueryRow executes a prepared query statement with the given arguments. If an
// error occurs during the execution of the statement, that error will be returned
// by a call to Scan on the returned *Row, which is always non-nil. If the query
// selects no rows, the *Row's Scan will return ErrNoRows. Otherwise, the *Row's
// Scan scans the first selected row and discards the rest.
//
// QueryRow prepares the same statement on another connection and queries it
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
