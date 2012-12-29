// This is a very basic example of a program that implements rdb.Parser and
// outputs a human readable diffable dump of the rdb file.
package main

import (
	"fmt"
	"os"

	"github.com/titanous/rdb"
)

type parser struct {
	db int
	i  int
}

func (p *parser) StartRDB() {
}

func (p *parser) StartDatabase(n int) {
	p.db = n
}

func (p *parser) EndDatabase(n int) {
}

func (p *parser) EndRDB() {
}

func (p *parser) Set(key, value []byte, expiry int64) {
	fmt.Printf("db=%d %q -> %q\n", p.db, key, value)
}

func (p *parser) StartHash(key []byte, length, expiry int64) {
}

func (p *parser) Hset(key, field, value []byte) {
	fmt.Printf("db=%d %q . %q -> %q\n", p.db, key, field, value)
}

func (p *parser) EndHash(key []byte) {
}

func (p *parser) StartSet(key []byte, cardinality, expiry int64) {
}

func (p *parser) Sadd(key, member []byte) {
	fmt.Printf("db=%d %q { %q }\n", p.db, key, member)
}

func (p *parser) EndSet(key []byte) {
}

func (p *parser) StartList(key []byte, length, expiry int64) {
	p.i = 0
}

func (p *parser) Rpush(key, value []byte) {
	fmt.Printf("db=%d %q[%d] -> %q\n", p.db, key, p.i, value)
	p.i++
}

func (p *parser) EndList(key []byte) {
}

func (p *parser) StartZSet(key []byte, cardinality, expiry int64) {
	p.i = 0
}

func (p *parser) Zadd(key []byte, score float64, member []byte) {
	fmt.Printf("db=%d %q[%d] -> {%q, score=%g}\n", p.db, key, p.i, member, score)
	p.i++
}

func (p *parser) EndZSet(key []byte) {
}

func maybeFatal(err error) {
	if err != nil {
		fmt.Printf("Fatal error: %s\n", err)
		os.Exit(1)
	}
}

func main() {
	f, err := os.Open(os.Args[1])
	maybeFatal(err)
	err = rdb.Parse(f, &parser{})
	maybeFatal(err)
}
