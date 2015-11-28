package ctxdb

import (
	"os"
	"testing"

	_ "github.com/lib/pq"
)

func getConn(t *testing.T) *DB {
	p, err := Open(
		os.Getenv("NISQL_TEST_DIALECT"),
		os.Getenv("NISQL_TEST_DSN"),
	)

	if err != nil {
		t.Errorf("open error: %s", err)
	}

	return p
}

func TestGetFromPool(t *testing.T) {
	p := getConn(t)

	conn, err := p.getFromPool()
	if err != nil {
		t.Errorf("Get error: %s", err)
	}

	if conn == nil {
		t.Errorf("conn is nil")
	}

	close(p.conns)

	conn, err = p.getFromPool()
	if err != ErrClosed {
		t.Errorf("Error should be ErrClosed, got: %s", err)
	}
}

func TestGetFromPoolClosed(t *testing.T) {
	p := getConn(t)

	if err := p.Close(); err != nil {
		t.Errorf("Err while closing the connection: %# v", err)
	}

	_, err := p.getFromPool()
	if err != ErrClosed {
		t.Errorf("Error should be ErrClosed, got: %s", err)
	}
}

func TestPutPoolNilConn(t *testing.T) {
	p := getConn(t)

	if err := p.put(nil); err != ErrNilConn {
		t.Errorf("Err should be NilConn, got: %# v", err)
	}
}

func TestPutPool(t *testing.T) {
	p := getConn(t)

	conn, err := p.getFromPool()
	if err != nil {
		t.Errorf("Error should be nil, got: %s", err)
	}

	if conn == nil {
		t.Errorf("conn is nil")
	}

	if err := p.put(conn); err != nil {
		t.Errorf("Err while putting the connection: %# v", err)
	}
}

func TestPutPoolClosedConn(t *testing.T) {
	p := getConn(t)

	conn, err := p.getFromPool()
	if err != nil {
		t.Errorf("Error should be nil, got: %s", err)
	}

	if conn == nil {
		t.Errorf("conn is nil")
	}

	if err := p.Close(); err != nil {
		t.Errorf("Err while closing the connection: %# v", err)
	}

	if err := p.put(conn); err != nil {
		t.Errorf("Err while putting the connection: %# v", err)
	}

	if err := conn.Ping(); err != nil && err.Error() != "sql: database is closed" {
		t.Errorf("conn should be closed: got %# v", err)
	}

	if err := p.put(nil); err != ErrNilConn {
		t.Errorf("Expected ErrNilConn, got: %# v", err)
	}
}

func TestPutPoolClosedPool(t *testing.T) {
	p := getConn(t)

	conn, err := p.getFromPool()
	if err != nil {
		t.Errorf("Error should be nil, got: %s", err)
	}

	if conn == nil {
		t.Errorf("conn is nil")
	}

	conns := p.getConns()
	p.conns = nil

	if err := p.put(conn); err != nil {
		t.Errorf("Err while putting the connection: %# v", err)
	}

	if err := conn.Ping(); err != nil && err.Error() != "sql: database is closed" {
		t.Errorf("conn should be closed: got %# v", err)
	}

	p.conns = conns
	if err := p.put(conn); err != nil {
		t.Errorf("Err while putting the connection: %# v", err)
	}
}

func TestPutPoolFull(t *testing.T) {
	p := getConn(t)

	conn1, err := p.getFromPool()
	if err != nil {
		t.Errorf("Error should be nil, got: %s", err)
	}

	conn2, err := p.getFromPool()
	if err != nil {
		t.Errorf("Error should be nil, got: %s", err)
	}

	conn3, err := p.getFromPool()
	if err != nil {
		t.Errorf("Error should be nil, got: %s", err)
	}

	if err := p.put(conn1); err != nil {
		t.Errorf("Err while putting the connection: %# v", err)
	}

	if err := p.put(conn2); err != nil {
		t.Errorf("Err while putting the connection: %# v", err)
	}

	if err := p.put(conn3); err != nil {
		t.Errorf("Err while putting the connection: %# v", err)
	}

	if err := conn3.Ping(); err != nil && err.Error() != "sql: database is closed" {
		t.Errorf("conn should be closed: got %# v", err)
	}
}

func TestClose(t *testing.T) {
	p := getConn(t)

	_, err := p.getFromPool()
	if err != nil {
		t.Errorf("Error should be nil, got: %s", err)
	}

	p.conns <- nil
	if err := p.Close(); err != nil {
		t.Errorf("Error should be nil while trying to close a nil connection, got: %s", err)
	}

	if err := p.Close(); err != ErrClosed {
		t.Errorf("Err should be Closed:  got %# v", err)
	}
}

func TestSetMaxOpenConns(t *testing.T) {
	p := getConn(t)
	p.SetMaxOpenConns(1)
}
