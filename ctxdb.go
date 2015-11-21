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

func (db *DB) get(ctx context.Context, f func(sqldb *sql.DB), done chan struct{}) error {
	select {
	case <-db.sem:

		// do not forget to put back
		defer func() {
			db.sem <- struct{}{}
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
			return db.put(sqldb)
		}

	case <-ctx.Done():
		// we could not get a connection sem in normal time
		return ctx.Err()
	}
}
