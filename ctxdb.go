package ctxdb

import (
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	"golang.org/x/net/context"
)

const maxOpenConns = 2

type DB struct {
	// maxIdleConns int
	maxOpenConns int
	sem          chan struct{}
	usageTimeout time.Duration

	usageTimeoutMux sync.RWMutex
	mu              sync.Mutex
	conns           chan *sql.DB

	factory Factory // sql.DB generator
}

type Factory func() (*sql.DB, error)

func Open(driver, dsn string) (*DB, error) {
	// We wrap *sql.DB into our DB
	db := &DB{
		maxOpenConns: maxOpenConns,
		sem:          make(chan struct{}, maxOpenConns),

		conns: make(chan *sql.DB, maxOpenConns),
		// usageTimeout: time.Second * 30,
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

var ErrTimedOut = errors.New("timed out")

func (db *DB) conn() (func(), error) {
	releaseLock := func() {}

	db.usageTimeoutMux.RLock()
	usageTimeout := db.usageTimeout
	db.usageTimeoutMux.RUnlock()

	timeOutCh := time.After(usageTimeout)

	select {
	case <-db.sem:
		// we aquired one connection, continue with that
	case <-timeOutCh:
		return nil, ErrTimedOut
		// we could not get a connection
	}

	releaseLock = func() {
		db.sem <- struct{}{}
	}

	cancelUsageTimeout := func() {}

	cancelTimeoutCh := make(chan struct{}, 1)
	cancelUsageTimeout = func() {
		cancelTimeoutCh <- struct{}{}
		close(cancelTimeoutCh)
	}

	go func() {
		select {
		case <-timeOutCh:
		case <-cancelTimeoutCh:
		}
	}()

	return func() {
		releaseLock()
		cancelUsageTimeout()
	}, nil
}

func (db *DB) Ping(ctx context.Context) error {
	sqldb, err := db.get(ctx)
	if err != nil {
		return err
	}

	// defer release()
	return sqldb.Ping()
}

func (db *DB) get(ctx context.Context) (*sql.DB, error) {

	sqldb, err := db.getFromPool()
	if err != nil {
		return nil, err
	}

	// Request cancelation changed in Go 1.5, see cancelreq.go and cancelreq_go14.go.
	cancel := func() {
		err := sqldb.Close()
		fmt.Println("err.Error()-->", err.Error())
	}

	type responseAndError struct {
		err error
	}
	result := make(chan responseAndError, 1)

	go func() {
		// resp, err := client.Do(req)
		result <- responseAndError{}
	}()

	select {
	case <-ctx.Done():
		cancel()
		return nil, ctx.Err()
	case r := <-result:
		return nil, r.err
	}
}
