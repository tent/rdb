# rdb [![Build Status](https://travis-ci.org/cupcake/rdb.png?branch=master)](https://travis-ci.org/titanous/rdb)

rdb is a Go package that implements parsing of the [Redis](http://redis.io) [RDB
file format](https://github.com/sripathikrishnan/redis-rdb-tools/blob/master/docs/RDB_File_Format.textile).

This package was heavily inspired by
[redis-rdb-tools](https://github.com/sripathikrishnan/redis-rdb-tools) by
[Sripathi Krishnan](https://github.com/sripathikrishnan).

## Installation

```
go get github.com/cupcake/rdb
```

## Functions

```go
func Decode(r io.Reader, d Decoder) error
```

Decode parses a RDB file from `r` and calls the decode hooks on `d`.

```go
func DecodeDump(dump []byte, db int, key []byte, expiry int64, d Decoder) error
```

Decode a byte slice from the Redis DUMP command. The dump does not contain the
database, key or expiry, so they must be included in the function call (but can
be zero values).

## Types

```go
type Decoder interface {
    // StartRDB is called when parsing of a valid RDB file starts.
    StartRDB()
    // StartDatabase is called when database n starts.
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

A Decoder must be implemented to parse a RDB file.
