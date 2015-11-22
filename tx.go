package ctxdb

import (
	"database/sql"

	"golang.org/x/net/context"
)

type Tx struct {
	tx    *sql.Tx
	sqldb *sql.DB
	db    *DB
}

// Commit commits the transaction.
func (tx *Tx) Commit(ctx context.Context) error {
	done := make(chan struct{}, 1)

	var err error
	go func() {
		err = tx.tx.Commit()
		close(done)
	}()

	select {
	case <-ctx.Done():
		if err := tx.sqldb.Close(); err != nil {
			return err
		}

		return ctx.Err()
	case <-done:
		if err := tx.db.put(tx.sqldb); err != nil {
			return err
		}

		return err
	}
}

// Exec executes a query that doesn't return rows. For example: an INSERT and
// UPDATE.
func (tx *Tx) Exec(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	done := make(chan struct{}, 1)

	var res sql.Result
	var err error

	go func() {
		res, err = tx.tx.Exec(query, args)
		close(done)
	}()

	select {
	case <-ctx.Done():
		if err := tx.Rollback(ctx); err != nil {
			return nil, err
		}

		return nil, ctx.Err()
	case <-done:
		return res, err
	}
}

// Prepare creates a prepared statement for use within a transaction.
//
// The returned statement operates within the transaction and can no longer be
// used once the transaction has been committed or rolled back.
//
// To use an existing prepared statement on this transaction, see Tx.Stmt.
func (tx *Tx) Prepare(ctx context.Context, query string) (*Stmt, error) {
	done := make(chan struct{}, 1)

	var res *sql.Stmt
	var err error

	go func(sqldb *sql.DB) {
		res, err = tx.tx.Prepare(query)
		close(done)
	}(tx.sqldb)

	select {
	case <-ctx.Done():
		if err := tx.Rollback(ctx); err != nil {
			return nil, err
		}

		return nil, ctx.Err()
	case <-done:
		return &Stmt{stmt: res}, err
	}
}

func (tx *Tx) Query(ctx context.Context, query string, args ...interface{}) (*Rows, error) {
	done := make(chan struct{}, 1)

	var res *sql.Rows
	var err error

	go func(sqldb *sql.DB) {
		res, err = tx.tx.Query(query, args...)
		close(done)
	}(tx.sqldb)

	select {
	case <-ctx.Done():
		if err := tx.Rollback(ctx); err != nil {
			return &Rows{
				rows:  res,
				err:   err,
				sqldb: tx.sqldb,
				db:    tx.db,
			}, err
		}

		return nil, ctx.Err()
	case <-done:
		return &Rows{
			rows:  res,
			err:   err,
			sqldb: tx.sqldb,
			db:    tx.db,
		}, err
	}
}

func (tx *Tx) QueryRow(ctx context.Context, query string, args ...interface{}) *Row {
	done := make(chan struct{}, 1)
	var res *sql.Row
	go func() {
		res = tx.tx.QueryRow(query, args...)
		close(done)
	}()

	select {
	case <-ctx.Done():
		r := &Row{
			sqldb: tx.sqldb,
			db:    tx.db,
		}
		if err := tx.Rollback(ctx); err != nil {
			r.err = err
			return r
		}
		r.err = ctx.Err()
		return r
	case <-done:
		return &Row{
			row:   res,
			sqldb: tx.sqldb,
			db:    tx.db,
		}
	}
}
func (tx *Tx) Rollback(ctx context.Context) error {
	done := make(chan struct{}, 1)

	var err error
	go func(sqldb *sql.DB) {
		err = tx.tx.Rollback()
		close(done)
	}(tx.sqldb)

	select {
	case <-ctx.Done():
		if err := tx.sqldb.Close(); err != nil {
			return err
		}

		return ctx.Err()
	case <-done:
		if err := tx.db.put(tx.sqldb); err != nil {
			return err
		}
		return err
	}
}

// Stmt returns a transaction-specific prepared statement from an existing
// statement.
// Example:
//
// ```
// updateMoney, err := db.Prepare("UPDATE balance SET money=money+? WHERE id=?")
// ...
// tx, err := db.Begin()
// ...
// res, err := tx.Stmt(updateMoney).Exec(123.45, 98293203)
// ```
//
// The returned statement operates within the transaction and can no longer be
// used once the transaction has been committed or rolled back.
func (tx *Tx) Stmt(ctx context.Context, stmt *Stmt) *Stmt {
	s := tx.tx.Stmt(stmt.stmt)
	return &Stmt{stmt: s}
}
