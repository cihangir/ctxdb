# ctxdb
[![GoDoc](https://godoc.org/github.com/cihangir/ctxdb?status.svg)](https://godoc.org/github.com/cihangir/ctxdb)
[![Build Status](https://travis-ci.org/cihangir/ctxdb.svg)](https://travis-ci.org/cihangir/ctxdb)

## Install

```
go install github.com/cihangir/ctxdb
```

## Usage

```
age := 27
rows, err := db.Query(ctx, "SELECT id FROM users WHERE age = $1", age)
if err != nil {
    log.Fatal(err)
}

for rows.Next(ctx) {
    var id int64
    if err := rows.Scan(ctx, &id); err != nil {
        log.Fatal(err)
    }

    fmt.Printf("id is %d\n", id)
}

if err := rows.Err(); err != nil {
    log.Fatal(err)
}

if err := rows.Close(ctx); err != nil {
    log.Fatal(err)
}
```
