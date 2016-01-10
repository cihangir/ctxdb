package ctxdb

import (
	"database/sql"
	"errors"
)

var (
	// ErrClosed represents closed ctxdb error
	ErrClosed = errors.New("connection is closed")

	// ErrNilConn represents given nil connection error TODO(cihangir) panic maybe
	ErrNilConn = errors.New("connection is nil. rejecting")

	// ErrMaxConnLimitReached represents overuse of connections
	ErrMaxConnLimitReached = errors.New("connection limit reached")
)

// func (db *DB) SetMaxIdleConns(i int) {
// 	db.mu.Lock()
// 	db.maxIdleConns = i
// 	db.confMu.Unlock()
// }

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
