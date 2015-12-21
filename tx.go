package ctxdb

import (
	"database/sql"
	"sync"

	"golang.org/x/net/context"
)

// Tx is an in-progress database transaction.
//
// A transaction must end with a call to Commit or Rollback.
//
// After a call to Commit or Rollback, all operations on the transaction fail
// with ErrTxDone.
//
// The statements prepared for a transaction by calling the transaction's
// Prepare or Stmt methods are closed by the call to Commit or Rollback.
type Tx struct {
	tx        *sql.Tx
	sqldb     *sql.DB
	db        *DB
	stickyErr error

	sync.Mutex
}

func (tx *Tx) shutdown() error {
	rollbackErr := tx.tx.Rollback()
	return tx.db.restoreOrClose(rollbackErr, tx.sqldb)
}

// Commit commits the transaction.
//
// If previous operations caused a sticky error returns it otherwise uses the
// given ctx and its deadline to signal timeouts. On timeout or cancel case,
// closes the underlying connection.
func (tx *Tx) Commit(ctx context.Context) error {
	tx.Lock()
	defer tx.Unlock()

	if tx.stickyErr != nil {
		return tx.stickyErr
	}

	done := make(chan struct{}, 1)

	var err error
	f := func() {
		err = tx.tx.Commit()
		close(done)
	}

	if err := tx.db.processWithGivenSQL(ctx, f, done, tx.sqldb); err != nil {
		return err
	}

	return err
}

// Exec executes a query that doesn't return rows. For example: an INSERT and
// UPDATE.
//
// If previous operations caused a sticky error returns it otherwise uses the
// given ctx and its deadline to signal timeouts. On timeout or cancel case,
// first tries to rollback the transaction then closes the underlying
// connection. Transaction Rollback error is omitted if the Connection Close
// returns an error. Operation error is omitted if the Rollback operation
// returns an error.
func (tx *Tx) Exec(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	tx.Lock()
	defer tx.Unlock()

	if tx.stickyErr != nil {
		return nil, tx.stickyErr
	}

	done := make(chan struct{}, 1)

	var res sql.Result
	var err error

	go func() {
		res, err = tx.tx.Exec(query, args)
		close(done)
	}()

	select {
	case <-ctx.Done():
		if err := tx.shutdown(); err != nil {
			tx.stickyErr = err
			return nil, err
		}

		tx.stickyErr = ctx.Err()
		return nil, tx.stickyErr
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
//
// If previous operations caused a sticky error returns it otherwise uses the
// given ctx and its deadline to signal timeouts. On timeout or cancel case,
// first tries to rollback the transaction then closes the underlying
// connection. Transaction Rollback error is omitted if the Connection Close
// returns an error. Operation error is omitted if the Rollback operation
// returns an error.
func (tx *Tx) Prepare(ctx context.Context, query string) (*Stmt, error) {
	tx.Lock()
	defer tx.Unlock()

	if tx.stickyErr != nil {
		return nil, tx.stickyErr
	}

	done := make(chan struct{}, 1)

	var res *sql.Stmt
	var err error

	go func() {
		res, err = tx.tx.Prepare(query)
		close(done)
	}()

	select {
	case <-ctx.Done():
		if err := tx.shutdown(); err != nil {
			tx.stickyErr = err
			return nil, err
		}

		tx.stickyErr = ctx.Err()
		return nil, tx.stickyErr
	case <-done:
		return &Stmt{stmt: res}, err
	}
}

// Query executes a query that returns rows, typically a SELECT. The args are
// for any placeholder parameters in the query.
//
// If previous operations caused a sticky error returns it otherwise uses the
// given ctx and its deadline to signal timeouts. On timeout or cancel case,
// first tries to rollback the transaction then closes the underlying
// connection. Transaction Rollback error is omitted if the Connection Close
// returns an error. Operation error is omitted if the Rollback operation
// returns an error.
func (tx *Tx) Query(ctx context.Context, query string, args ...interface{}) (*Rows, error) {
	tx.Lock()
	defer tx.Unlock()

	if tx.stickyErr != nil {
		return nil, tx.stickyErr
	}

	done := make(chan struct{}, 1)

	var res *sql.Rows
	var err error

	go func() {
		res, err = tx.tx.Query(query, args...)
		close(done)
	}()

	select {
	case <-ctx.Done():
		if err := tx.shutdown(); err != nil {
			tx.stickyErr = err
			return nil, err
		}

		tx.stickyErr = ctx.Err()
		return nil, tx.stickyErr
	case <-done:
		if err != nil {
			return nil, err
		}

		return &Rows{
			rows:  res,
			sqldb: tx.sqldb,
			db:    tx.db,
		}, nil
	}
}

// QueryRow executes a query that is expected to return at most one row.
// QueryRow always return a non-nil value. Errors are deferred until Row's Scan
// method is called.
//
// If previous operations caused a sticky error returns it otherwise uses the
// given ctx and its deadline to signal timeouts. On timeout or cancel case,
// first tries to rollback the transaction then closes the underlying
// connection. Transaction Rollback error is omitted if the Connection Close
// returns an error. Operation error is omitted if the Rollback operation
// returns an error.
func (tx *Tx) QueryRow(ctx context.Context, query string, args ...interface{}) *Row {
	tx.Lock()
	defer tx.Unlock()

	if tx.stickyErr != nil {
		return &Row{sqldb: tx.sqldb, db: tx.db, err: tx.stickyErr}
	}

	done := make(chan struct{}, 1)
	var res *sql.Row
	go func() {
		res = tx.tx.QueryRow(query, args...)
		close(done)
	}()

	select {
	case <-ctx.Done():
		err := ctx.Err()
		// prepare non-nil Query
		r := &Row{sqldb: tx.sqldb, db: tx.db, err: err}
		tx.stickyErr = err

		if err := tx.shutdown(); err != nil {
			tx.stickyErr = err
			r.err = err
		}

		return r
	case <-done:
		return &Row{
			row:   res,
			sqldb: tx.sqldb,
			db:    tx.db,
		}
	}
}

// Rollback aborts the transaction.
func (tx *Tx) Rollback(ctx context.Context) error {
	tx.Lock()
	defer tx.Unlock()

	if tx.stickyErr != nil {
		return tx.stickyErr
	}

	done := make(chan struct{}, 1)

	var err error

	f := func() {
		err = tx.tx.Rollback()
		close(done)
	}

	if err := tx.db.processWithGivenSQL(ctx, f, done, tx.sqldb); err != nil {
		return err
	}

	return err
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
	tx.Lock()
	defer tx.Unlock()

	if tx.stickyErr != nil {
		return &Stmt{err: tx.stickyErr}
	}

	s := tx.tx.Stmt(stmt.stmt)
	return &Stmt{stmt: s}
}
