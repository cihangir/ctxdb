package ctxdb

import (
	"database/sql"
	"errors"
	"time"
)

var (
	ErrClosed              = errors.New("connection is closed")
	ErrNilConn             = errors.New("connection is nil. rejecting")
	ErrMaxConnLimitReached = errors.New("connection limit reached")
)

// func (db *DB) SetMaxIdleConns(i int) {
// 	db.mu.Lock()
// 	db.maxIdleConns = i
// 	db.confMu.Unlock()
// }

func (db *DB) SetMaxOpenConns(i int) {
	db.mu.Lock()
	db.maxOpenConns = i
	db.mu.Unlock()
}

func (db *DB) SetUsageTimeout(timeout time.Duration) {
	db.mu.Lock()
	db.usageTimeout = timeout
	db.mu.Unlock()
}

func (db *DB) getConns() chan *sql.DB {
	db.mu.Lock()
	conns := db.conns
	db.mu.Unlock()
	return conns
}

func (db *DB) getFromPool() (*sql.DB, error) {
	conns := db.getConns()
	if conns == nil {
		return nil, ErrClosed
	}

	select {
	case conn := <-conns:
		if conn == nil {
			return nil, ErrClosed
		}

		return conn, nil
	default:
		return db.factory()
	}
}

func (db *DB) put(conn *sql.DB) error {
	if conn == nil {
		return ErrNilConn
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	if db.conns == nil {
		// pool is closed, close passed connection
		return conn.Close()
	}

	select {
	case db.conns <- conn:
		return nil
	default:
		// pool is full, close passed connection
		return conn.Close()
	}
}

// Close closes the all connections
func (db *DB) Close() error {
	db.mu.Lock()
	conns := db.conns
	db.conns = nil
	db.factory = nil
	db.mu.Unlock()

	if conns == nil {
		return ErrClosed
	}

	close(conns)

	for conn := range conns {
		if conn == nil {
			continue
		}

		if err := conn.Close(); err != nil {
			return err
		}
	}

	return nil
}
