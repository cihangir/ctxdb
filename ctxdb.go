package ctxdb

import (
	"database/sql"
	"sync"

	"golang.org/x/net/context"
)

const maxOpenConns = 2

type DB struct {
	// maxIdleConns int
	maxOpenConns int
	sem          chan struct{}

	mu    sync.Mutex
	conns chan *sql.DB

	factory Factory // sql.DB generator
}

type Factory func() (*sql.DB, error)

// Open opens a database specified by its database driver name and a driver-
// specific data source name, usually consisting of at least a database name and
// connection information.

// Most users will open a database via a driver-specific connection helper
// function that returns a *DB. No database drivers are included in the Go
// standard library. See https://golang.org/s/sqldrivers for a list of third-
// party drivers.

// Open may just validate its arguments without creating a connection to the
// database. To verify that the data source name is valid, call Ping.

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

	r := &Row{}

// handleWithSQL accepts context for deadlines, f for operation, and done
// channel for signalling operation, if an error occurs while operating, closes
// the underlying database connection immediately, and signals the sem chan for
// recycling a new db. If operation is successfull, returns the underlying db
// connection, receiver must handle the sem communication and db lifecycle
	select {
	case <-db.sem:

		// do not forget to put back
		defer func() {
			// db is not inuse anymore
			if r != nil && r.err != nil {
				select {
				case db.sem <- struct{}{}:
				default:
					panic("overflow 2-->")
				}
			}
		}()

		// we aquired one connection sem, continue with that
		sqldb, err := db.getFromPool()
		if err != nil {
			r.err = err
			return r
		}

		done := make(chan struct{}, 0)
		var res *sql.Row

		go func() {
			res = sqldb.QueryRow(query, args...)
			close(done)
		}()

		select {
		case <-ctx.Done():

			r = &Row{
				row:   res,
				err:   ctx.Err(),
				sqldb: sqldb,
				db:    db,
			}

			if err := sqldb.Close(); err != nil {
				r.err = err
			}

			return r
		case <-done:
			r = &Row{
				row:   res,
				sqldb: sqldb,
				db:    db,
			}
			return r
		}

	case <-ctx.Done():
		// we could not get a connection sem in normal time
		r.err = ctx.Err()
		r.db = db
		return r
	}
}

func (db *DB) process(ctx context.Context, f func(sqldb *sql.DB), done chan struct{}) error {
	select {
	case <-db.sem:
		// do not forget to put back connection
		defer func() {
			select {
			case db.sem <- struct{}{}:
			default:
				panic("overflow 3-->")
			}
		}()

		// we aquired one connection sem, continue with that
		sqldb, err := db.getFromPool()
		if err != nil {
			return err
		}

		go f(sqldb)

		select {
		case <-ctx.Done():
			if err := sqldb.Close(); err != nil {
				return err
			}

			return ctx.Err()
		case <-done:
			err := db.put(sqldb)
			return err
		}

	case <-ctx.Done():
		// we could not get a connection sem in normal time
		err := ctx.Err()
		return err
	}
}
