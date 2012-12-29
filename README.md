# rdb

rdb is a Go package that implements parsing of the [Redis](http://redis.io) [RDB
file format](https://github.com/sripathikrishnan/redis-rdb-tools/blob/master/docs/RDB_File_Format.textile).

This package was heavily inspired by
[redis-rdb-tools](https://github.com/sripathikrishnan/redis-rdb-tools) by
[Sripathi Krishnan](https://github.com/sripathikrishnan).

## Installation

```
go get github.com/titanous/rdb
```

## Functions

```go
func Parse(r io.Reader, p Parser) error
```

Parse parses a RDB file from `r` and calls the parse hooks on `p`.

## Types

```go
type Parser interface {
  	// StartRDB is called when parsing of a valid RDB file starts.
    StartRDB()
    // StartDatabase is called before database n is started.
    // Once a database starts, another database will not start until EndDatabase is called.
    StartDatabase(n int)
    // Set is called once for each string key.
    Set(key, value []byte, expiry int64)
    // StartHash is called at the beginning of a hash.
    // Hset will be called exactly length times before EndHash.
    StartHash(key []byte, length, expiry int64)
    // Hset is called once for each field=value pair in a hash.
    Hset(key, field, value []byte)
    // EndHash is called when there are no more fields in a hash.
    EndHash(key []byte)
    // StartSet is called at the beginning of a set.
    // Sadd will be called exactly cardinality times before EndSet.
    StartSet(key []byte, cardinality, expiry int64)
    // Sadd is called once for each member of a set.
    Sadd(key, member []byte)
    // EndSet is called when there are no more fields in a set.
    EndSet(key []byte)
    // StartList is called at the beginning of a list.
    // Rpush will be called exactly length times before EndList.
    StartList(key []byte, length, expiry int64)
    // Rpush is called once for each value in a list.
    Rpush(key, value []byte)
    // EndList is called when there are no more values in a list.
    EndList(key []byte)
    // StartZSet is called at the beginning of a sorted set.
    // Zadd will be called exactly cardinality times before EndZSet.
    StartZSet(key []byte, cardinality, expiry int64)
    // Zadd is called once for each member of a sorted set.
    Zadd(key []byte, score float64, member []byte)
    // EndZSet is called when there are no more members in a sorted set.
    EndZSet(key []byte)
    // EndDatabase is called at the end of a database.
    EndDatabase(n int)
    // EndRDB is called when parsing of the RDB file is complete.
    EndRDB()
}
```

A Parser must be implemented to parse a RDB file.
