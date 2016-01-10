// +build go1.5

package ctxdb

import "database/sql"

// Stats returns database statistics.
func (db *DB) Stats() sql.DBStats {
	done := make(chan struct{}, 1)

	var res sql.DBStats

	f := func(sqldb *sql.DB) {
		res = sqldb.Stats()
		close(done)
	}

	if err := db.process(ctx, f, done); err != nil {
		panic(err) //TODO(cihangir) panic is overkill
	}

	return res
}
