package ctxdb

import (
	"database/sql"
	"errors"
	"sync"

	"golang.org/x/net/context"
)

const maxOpenConns = 2

// DB is a database handle representing a pool of zero or more underlying
// connections. It's safe for concurrent use by multiple goroutines.
type DB struct {
	// maxIdleConns int
	maxOpenConns int
	sem          chan struct{}

	mu    sync.Mutex
	conns chan *sql.DB

	factory Factory // sql.DB generator
}

// Factory holds db generator
type Factory func() (*sql.DB, error)

// Open opens a database specified by its database driver name and a driver-
// specific data source name, usually consisting of at least a database name and
// connection information.
//
// Most users will open a database via a driver-specific connection helper
// function that returns a *DB. No database drivers are included in the Go
// standard library. See https://golang.org/s/sqldrivers for a list of third-
// party drivers.
//
// Open may just validate its arguments without creating a connection to the
// database. To verify that the data source name is valid, call Ping.
//
// The returned DB is safe for concurrent use by multiple goroutines and
// maintains its own pool of idle connections. Thus, the Open function should be
// called just once. It is rarely necessary to close a DB.
func Open(driver, dsn string) (*DB, error) {
	// We wrap *sql.DB into our DB
	db := &DB{
		maxOpenConns: maxOpenConns,
		sem:          make(chan struct{}, maxOpenConns),

		conns: make(chan *sql.DB, maxOpenConns),
		factory: func() (*sql.DB, error) {
			d, err := sql.Open(driver, dsn)
			if err != nil {
				return nil, err
			}

			d.SetMaxIdleConns(1)
			d.SetMaxOpenConns(1)
			return d, nil
		},
	}

	for i := 0; i < maxOpenConns; i++ {
		db.sem <- struct{}{}
	}

	return db, nil
}

// Ping verifies a connection to the database is still alive, establishing a
// connection if necessary.
func (db *DB) Ping(ctx context.Context) error {
	done := make(chan struct{}, 1)

	var err error

	f := func(sqldb *sql.DB) {
		err = sqldb.Ping()
		close(done)
	}

	if err := db.process(ctx, f, done); err != nil {
		return err
	}

	return nil
}

// Begin starts a transaction. The isolation level is dependent on the driver.
func (db *DB) Begin(ctx context.Context) (*Tx, error) {
	done := make(chan struct{}, 1)

	var err error
	var tx *sql.Tx
	f := func(sqldb *sql.DB) {
		tx, err = sqldb.Begin()
		close(done)
	}

	sqldb, opErr := db.handleWithSQL(ctx, f, done)
	if opErr != nil {
		return nil, opErr
	}

	return &Tx{
		tx:    tx,
		sqldb: sqldb,
		db:    db,
	}, nil
}

// Exec executes a query without returning any rows. The args are for any
// placeholder parameters in the query.
func (db *DB) Exec(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	done := make(chan struct{}, 1)

	var res sql.Result
	var err error

	f := func(sqldb *sql.DB) {
		res, err = sqldb.Exec(query, args...)
		close(done)
	}

	if err := db.process(ctx, f, done); err != nil {
		return nil, err
	}

	return res, err
}

// QueryRow executes a query that is expected to return at most one row.
// QueryRow always return a non-nil value. Errors are deferred until Row's Scan
// method is called.
func (db *DB) QueryRow(ctx context.Context, query string, args ...interface{}) *Row {
	done := make(chan struct{}, 0)

	var res *sql.Row

	f := func(sqldb *sql.DB) {
		res = sqldb.QueryRow(query, args...)
		close(done)
	}

	sqldb, err := db.handleWithSQL(ctx, f, done)
	if err != nil {
		return &Row{err: err}
	}

	return &Row{
		row:   res,
		sqldb: sqldb,
		db:    db,
	}
}

// Query executes a query that returns rows, typically a SELECT. The args are
// for any placeholder parameters in the query.
func (db *DB) Query(ctx context.Context, query string, args ...interface{}) (*Rows, error) {
	done := make(chan struct{}, 0)
	var res *sql.Rows
	var queryErr error
	f := func(sqldb *sql.DB) {
		res, queryErr = sqldb.Query(query, args...)
		close(done)
	}

	sqldb, err := db.handleWithSQL(ctx, f, done)
	if err != nil {
		return nil, err
	}

	if queryErr != nil {
		return nil, queryErr
	}

	return &Rows{
		rows:  res,
		sqldb: sqldb,
		db:    db,
	}, nil

}

// Prepare creates a prepared statement for later queries or executions.
// Multiple queries or executions may be run concurrently from the returned
// statement. The caller must call the statement's Close method when the
// statement is no longer needed.
func (db *DB) Prepare(ctx context.Context, query string) (*Stmt, error) {
	done := make(chan struct{}, 0)
	var res *sql.Stmt
	var queryErr error
	f := func(sqldb *sql.DB) {
		res, queryErr = sqldb.Prepare(query)
		close(done)
	}

	sqldb, err := db.handleWithSQL(ctx, f, done)
	if err != nil {
		return nil, err
	}

	if queryErr != nil {
		return nil, queryErr
	}

	return &Stmt{
		stmt:  res,
		query: query,
		sqldb: sqldb,
		db:    db,
	}, nil

}

// process accepts context for deadlines, f for operation, and done channel for
// signalling operation. At the end of the operation, puts db back to pool and
// increments the sem
func (db *DB) process(ctx context.Context, f func(sqldb *sql.DB), done chan struct{}) error {
	sqldb, err := db.handleWithSQL(ctx, f, done)
	if err != nil {
		return err
	}

	return db.restoreOrClose(nil, sqldb)
}

// handleWithSQL accepts context for deadlines, f for operation, and done
// channel for signalling operation, if an error occurs while operating, closes
// the underlying database connection immediately, and signals the sem chan for
// recycling a new db. If operation is successfull, returns the underlying db
// connection, receiver must handle the sem communication and db lifecycle
func (db *DB) handleWithSQL(ctx context.Context, f func(sqldb *sql.DB), done chan struct{}) (*sql.DB, error) {
	select {
	case <-db.sem:
		var err error

		defer func() {
			// db is not inuse anymore
			if err != nil {
				select {
				case db.sem <- struct{}{}:
				default:
					panic("sem overflow 5")
				}
			}
		}()

		// we aquired one connection sem, continue with that
		sqldb, err := db.getFromPool()
		if err != nil {
			return nil, err
		}

		fn := func() { f(sqldb) }

		err = db.handleWithGivenSQL(ctx, fn, done, sqldb)
		if err != nil {
			return nil, err
		}

		return sqldb, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (db *DB) processWithGivenSQL(ctx context.Context, f func(), done chan struct{}, sqldb *sql.DB) error {
	err := db.handleWithGivenSQL(ctx, f, done, sqldb)
	return db.restoreOrClose(err, sqldb)
}

// handleWithGivenSQL closes the given db connection if given context return an
// error while executing the give f func
func (db *DB) handleWithGivenSQL(ctx context.Context, f func(), done chan struct{}, sqldb *sql.DB) error {
	var err error

	go f()

	select {
	case <-ctx.Done():
		err = sqldb.Close()
		if err != nil {
			return err
		}

		err = ctx.Err()
		return err
	case <-done:
		return nil
	}

}

func (db *DB) restoreOrClose(err error, sqldb *sql.DB) error {
	select {
	case db.sem <- struct{}{}:
		if err == nil {
			return db.put(sqldb)
		}

		// Close is idempotent
		if err := sqldb.Close(); err != nil {
			return err
		}

		return err

	default:
		return errors.New("sem overflow in restoreOrClose")
	}
}
