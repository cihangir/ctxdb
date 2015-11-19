package ctxdb

import (
	"database/sql"
	"errors"
	"time"
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
	c.mu.Lock()
	conns := c.conns
	c.mu.Unlock()
	return conns
}

var ErrClosed = errors.New("connection is closed")

func (db *DB) getFromPool() (*sql.DB, error) {
	conns := c.getConns()
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
		return errors.New("connection is nil. rejecting")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// pool is closed, close passed connection
	if c.conns == nil {
		return conn.Close()
	}

	select {
	case c.conns <- conn:
		return nil
	default:
		// pool is full, close passed connection
		return conn.Close()
	}
}

func (db *DB) Close() {
	c.mu.Lock()
	conns := c.conns
	c.conns = nil
	c.factory = nil
	c.mu.Unlock()

	if conns == nil {
		return
	}

	close(conns)
	for conn := range conns {
		conn.Close()
	}
}
