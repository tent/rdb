// Package rdb implements parsing of the Redis RDB file format.
//
// This package is based heavily on redis-rdb-tools by Sripathi Krishnan: https://github.com/sripathikrishnan/redis-rdb-tools
package rdb

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"strconv"

	"github.com/titanous/rdb/crc64"
)

// A Parser must be implemented to parse a RDB file.
type Parser interface {
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

// Parse parses a RDB file from r and calls the parse hooks on p.
func Parse(r io.Reader, p Parser) error {
	parser := &parse{p, make([]byte, 8), bufio.NewReader(r)}
	return parser.parse()
}

// Parse a byte slice from the Redis DUMP command. The dump does not contain the
// database, key or expiry, so they must be included in the function call (but
// can be zero values).
func ParseDump(dump []byte, db int, key []byte, expiry int64, p Parser) error {
	err := verifyDump(dump)
	if err != nil {
		return err
	}

	parser := &parse{p, make([]byte, 8), bytes.NewReader(dump[1:])}
	parser.event.StartRDB()
	parser.event.StartDatabase(db)

	err = parser.readObject(key, dump[0], expiry)

	parser.event.EndDatabase(db)
	parser.event.EndRDB()
	return err
}

type byteReader interface {
	io.Reader
	io.ByteReader
}

type parse struct {
	event  Parser
	intBuf []byte
	r      byteReader
}

const (
	rdb6bitLen  = 0
	rdb14bitLen = 1
	rdb32bitLen = 2
	rdbEncVal   = 3

	rdbFlagExpiryMS = 0xfc
	rdbFlagExpiry   = 0xfd
	rdbFlagSelectDB = 0xfe
	rdbFlagEOF      = 0xff

	rdbTypeString = 0
	rdbTypeList   = 1
	rdbTypeSet    = 2
	rdbTypeZSet   = 3
	rdbTypeHash   = 4

	rdbTypeHashZipmap  = 9
	rdbTypeListZiplist = 10
	rdbTypeSetIntset   = 11
	rdbTypeZSetZiplist = 12
	rdbTypeHashZiplist = 13

	rdbEncInt8  = 0
	rdbEncInt16 = 1
	rdbEncInt32 = 2
	rdbEncLZF   = 3

	rdbZiplist6bitlenString  = 0
	rdbZiplist14bitlenString = 1
	rdbZiplist32bitlenString = 2

	rdbZiplistInt16 = 0xc0
	rdbZiplistInt32 = 0xd0
	rdbZiplistInt64 = 0xe0
	rdbZiplistInt24 = 0xf0
	rdbZiplistInt8  = 0xfe
	rdbZiplistInt4  = 15
)

func (p *parse) parse() error {
	err := p.checkHeader()
	if err != nil {
		return err
	}

	p.event.StartRDB()

	var db uint32
	var expiry int64
	firstDB := true
	for {
		objType, err := p.r.ReadByte()
		if err != nil {
			return err
		}
		switch objType {
		case rdbFlagExpiryMS:
			_, err := io.ReadFull(p.r, p.intBuf)
			if err != nil {
				return err
			}
			expiry = int64(binary.LittleEndian.Uint64(p.intBuf))
		case rdbFlagExpiry:
			_, err := io.ReadFull(p.r, p.intBuf[:4])
			if err != nil {
				return err
			}
			expiry = int64(binary.LittleEndian.Uint32(p.intBuf)) * 1000
		case rdbFlagSelectDB:
			if !firstDB {
				p.event.EndDatabase(int(db))
			}
			db, _, err = p.readLength()
			if err != nil {
				return err
			}
			p.event.StartDatabase(int(db))
		case rdbFlagEOF:
			p.event.EndDatabase(int(db))
			p.event.EndRDB()
			return nil
		default:
			key, err := p.readString()
			if err != nil {
				return err
			}
			err = p.readObject(key, objType, expiry)
			if err != nil {
				return err
			}
		}
	}

	panic("not reached")
}

func (p *parse) readObject(key []byte, typ byte, expiry int64) error {
	switch typ {
	case rdbTypeString:
		value, err := p.readString()
		if err != nil {
			return err
		}
		p.event.Set(key, value, expiry)
	case rdbTypeList:
		length, _, err := p.readLength()
		if err != nil {
			return err
		}
		p.event.StartList(key, int64(length), expiry)
		for i := uint32(0); i < length; i++ {
			value, err := p.readString()
			if err != nil {
				return err
			}
			p.event.Rpush(key, value)
		}
		p.event.EndList(key)
	case rdbTypeSet:
		cardinality, _, err := p.readLength()
		if err != nil {
			return err
		}
		p.event.StartSet(key, int64(cardinality), expiry)
		for i := uint32(0); i < cardinality; i++ {
			member, err := p.readString()
			if err != nil {
				return err
			}
			p.event.Sadd(key, member)
		}
		p.event.EndSet(key)
	case rdbTypeZSet:
		cardinality, _, err := p.readLength()
		if err != nil {
			return err
		}
		p.event.StartZSet(key, int64(cardinality), expiry)
		for i := uint32(0); i < cardinality; i++ {
			member, err := p.readString()
			if err != nil {
				return err
			}
			score, err := p.readFloat64()
			if err != nil {
				return err
			}
			p.event.Zadd(key, score, member)
		}
		p.event.EndZSet(key)
	case rdbTypeHash:
		length, _, err := p.readLength()
		if err != nil {
			return err
		}
		p.event.StartHash(key, int64(length), expiry)
		for i := uint32(0); i < length; i++ {
			field, err := p.readString()
			if err != nil {
				return err
			}
			value, err := p.readString()
			if err != nil {
				return err
			}
			p.event.Hset(key, field, value)
		}
		p.event.EndHash(key)
	case rdbTypeHashZipmap:
		return p.readZipmap(key, expiry)
	case rdbTypeListZiplist:
		return p.readZiplist(key, expiry)
	case rdbTypeSetIntset:
		return p.readIntset(key, expiry)
	case rdbTypeZSetZiplist:
		return p.readZiplistZset(key, expiry)
	case rdbTypeHashZiplist:
		return p.readZiplistHash(key, expiry)
	default:
		return fmt.Errorf("rdb: unknown object type %d for key %s", typ, key)
	}
	return nil
}

func (p *parse) readZipmap(key []byte, expiry int64) error {
	var length int
	zipmap, err := p.readString()
	if err != nil {
		return err
	}
	buf := newSliceBuffer(zipmap)
	lenByte, err := buf.ReadByte()
	if err != nil {
		return err
	}
	if lenByte >= 254 { // we need to count the items manually
		length, err = countZipmapItems(buf)
		length /= 2
		if err != nil {
			return err
		}
	} else {
		length = int(lenByte)
	}
	p.event.StartHash(key, int64(length), expiry)
	for i := 0; i < length; i++ {
		field, err := readZipmapItem(buf, false)
		if err != nil {
			return err
		}
		value, err := readZipmapItem(buf, true)
		if err != nil {
			return err
		}
		p.event.Hset(key, field, value)
	}
	p.event.EndHash(key)
	return nil
}

func readZipmapItem(buf *sliceBuffer, readFree bool) ([]byte, error) {
	length, free, err := readZipmapItemLength(buf, readFree)
	if err != nil {
		return nil, err
	}
	if length == -1 {
		return nil, nil
	}
	value, err := buf.Slice(length)
	if err != nil {
		return nil, err
	}
	_, err = buf.Seek(int64(free), 1)
	return value, err
}

func countZipmapItems(buf *sliceBuffer) (int, error) {
	n := 0
	for {
		strLen, free, err := readZipmapItemLength(buf, n%2 != 0)
		if err != nil {
			return 0, err
		}
		if strLen == -1 {
			break
		}
		_, err = buf.Seek(int64(strLen)+int64(free), 1)
		if err != nil {
			return 0, err
		}
		n++
	}
	_, err := buf.Seek(0, 0)
	return n, err
}

func readZipmapItemLength(buf *sliceBuffer, readFree bool) (int, int, error) {
	b, err := buf.ReadByte()
	if err != nil {
		return 0, 0, err
	}
	switch b {
	case 253:
		s, err := buf.Slice(5)
		if err != nil {
			return 0, 0, err
		}
		return int(binary.BigEndian.Uint32(s)), int(s[4]), nil
	case 254:
		return 0, 0, fmt.Errorf("rdb: invalid zipmap item length")
	case 255:
		return -1, 0, nil
	}
	var free byte
	if readFree {
		free, err = buf.ReadByte()
	}
	return int(b), int(free), err
}

func (p *parse) readZiplist(key []byte, expiry int64) error {
	ziplist, err := p.readString()
	if err != nil {
		return err
	}
	buf := newSliceBuffer(ziplist)
	length, err := readZiplistLength(buf)
	if err != nil {
		return err
	}
	p.event.StartList(key, length, expiry)
	for i := int64(0); i < length; i++ {
		entry, err := readZiplistEntry(buf)
		if err != nil {
			return err
		}
		p.event.Rpush(key, entry)
	}
	p.event.EndList(key)
	return nil
}

func (p *parse) readZiplistZset(key []byte, expiry int64) error {
	ziplist, err := p.readString()
	if err != nil {
		return err
	}
	buf := newSliceBuffer(ziplist)
	cardinality, err := readZiplistLength(buf)
	if err != nil {
		return err
	}
	cardinality /= 2
	p.event.StartZSet(key, cardinality, expiry)
	for i := int64(0); i < cardinality; i++ {
		member, err := readZiplistEntry(buf)
		if err != nil {
			return err
		}
		scoreBytes, err := readZiplistEntry(buf)
		if err != nil {
			return err
		}
		score, err := strconv.ParseFloat(string(scoreBytes), 64)
		if err != nil {
			return err
		}
		p.event.Zadd(key, score, member)
	}
	p.event.EndZSet(key)
	return nil
}

func (p *parse) readZiplistHash(key []byte, expiry int64) error {
	ziplist, err := p.readString()
	if err != nil {
		return err
	}
	buf := newSliceBuffer(ziplist)
	length, err := readZiplistLength(buf)
	if err != nil {
		return err
	}
	length /= 2
	p.event.StartHash(key, length, expiry)
	for i := int64(0); i < length; i++ {
		field, err := readZiplistEntry(buf)
		if err != nil {
			return err
		}
		value, err := readZiplistEntry(buf)
		if err != nil {
			return err
		}
		p.event.Hset(key, field, value)
	}
	p.event.EndHash(key)
	return nil
}

func readZiplistLength(buf *sliceBuffer) (int64, error) {
	buf.Seek(8, 0) // skip the zlbytes and zltail
	lenBytes, err := buf.Slice(2)
	if err != nil {
		return 0, err
	}
	return int64(binary.LittleEndian.Uint16(lenBytes)), nil
}

func readZiplistEntry(buf *sliceBuffer) ([]byte, error) {
	prevLen, err := buf.ReadByte()
	if err != nil {
		return nil, err
	}
	if prevLen == 254 {
		buf.Seek(4, 1) // skip the 4-byte prevlen
	}

	header, err := buf.ReadByte()
	if err != nil {
		return nil, err
	}
	switch {
	case header>>6 == rdbZiplist6bitlenString:
		return buf.Slice(int(header & 0x3f))
	case header>>6 == rdbZiplist14bitlenString:
		b, err := buf.ReadByte()
		if err != nil {
			return nil, err
		}
		return buf.Slice((int(header&0x3f) << 8) | int(b))
	case header>>6 == rdbZiplist32bitlenString:
		lenBytes, err := buf.Slice(4)
		if err != nil {
			return nil, err
		}
		return buf.Slice(int(binary.BigEndian.Uint32(lenBytes)))
	case header == rdbZiplistInt16:
		intBytes, err := buf.Slice(2)
		if err != nil {
			return nil, err
		}
		return []byte(strconv.FormatInt(int64(int16(binary.LittleEndian.Uint16(intBytes))), 10)), nil
	case header == rdbZiplistInt32:
		intBytes, err := buf.Slice(4)
		if err != nil {
			return nil, err
		}
		return []byte(strconv.FormatInt(int64(int32(binary.LittleEndian.Uint32(intBytes))), 10)), nil
	case header == rdbZiplistInt64:
		intBytes, err := buf.Slice(8)
		if err != nil {
			return nil, err
		}
		return []byte(strconv.FormatInt(int64(binary.LittleEndian.Uint64(intBytes)), 10)), nil
	case header == rdbZiplistInt24:
		intBytes := make([]byte, 4)
		_, err := buf.Read(intBytes[1:])
		if err != nil {
			return nil, err
		}
		return []byte(strconv.FormatInt(int64(int32(binary.LittleEndian.Uint32(intBytes))>>8), 10)), nil
	case header == rdbZiplistInt8:
		b, err := buf.ReadByte()
		return []byte(strconv.FormatInt(int64(int8(b)), 10)), err
	case header>>4 == rdbZiplistInt4:
		return []byte(strconv.FormatInt(int64(header&0x0f)-1, 10)), nil
	}

	return nil, fmt.Errorf("rdb: unknown ziplist header byte: %d", header)
}

func (p *parse) readIntset(key []byte, expiry int64) error {
	intset, err := p.readString()
	if err != nil {
		return err
	}
	buf := newSliceBuffer(intset)
	intSizeBytes, err := buf.Slice(4)
	if err != nil {
		return err
	}
	intSize := binary.LittleEndian.Uint32(intSizeBytes)

	if intSize != 2 && intSize != 4 && intSize != 8 {
		return fmt.Errorf("rdb: unknown intset encoding: %d", intSize)
	}

	lenBytes, err := buf.Slice(4)
	if err != nil {
		return err
	}
	cardinality := binary.LittleEndian.Uint32(lenBytes)

	p.event.StartSet(key, int64(cardinality), expiry)
	for i := uint32(0); i < cardinality; i++ {
		intBytes, err := buf.Slice(int(intSize))
		if err != nil {
			return err
		}
		var intString string
		switch intSize {
		case 2:
			intString = strconv.FormatInt(int64(int16(binary.LittleEndian.Uint16(intBytes))), 10)
		case 4:
			intString = strconv.FormatInt(int64(int32(binary.LittleEndian.Uint32(intBytes))), 10)
		case 8:
			intString = strconv.FormatInt(int64(int64(binary.LittleEndian.Uint64(intBytes))), 10)
		}
		p.event.Sadd(key, []byte(intString))
	}
	p.event.EndSet(key)
	return nil
}

func (p *parse) checkHeader() error {
	header := make([]byte, 9)
	_, err := io.ReadFull(p.r, header)
	if err != nil {
		return err
	}

	if !bytes.Equal(header[:5], []byte("REDIS")) {
		return fmt.Errorf("rdb: invalid file format")
	}

	version, _ := strconv.ParseInt(string(header[5:]), 10, 64)
	if version < 1 || version > 6 {
		return fmt.Errorf("rdb: invalid RDB version number %d", version)
	}

	return nil
}

func (p *parse) readString() ([]byte, error) {
	length, encoded, err := p.readLength()
	if err != nil {
		return nil, err
	}
	if encoded {
		switch length {
		case rdbEncInt8:
			i, err := p.readUint8()
			return []byte(strconv.FormatInt(int64(int8(i)), 10)), err
		case rdbEncInt16:
			i, err := p.readUint16()
			return []byte(strconv.FormatInt(int64(int16(i)), 10)), err
		case rdbEncInt32:
			i, err := p.readUint32()
			return []byte(strconv.FormatInt(int64(int32(i)), 10)), err
		case rdbEncLZF:
			clen, _, err := p.readLength()
			if err != nil {
				return nil, err
			}
			ulen, _, err := p.readLength()
			if err != nil {
				return nil, err
			}
			compressed := make([]byte, clen)
			_, err = io.ReadFull(p.r, compressed)
			if err != nil {
				return nil, err
			}
			decompressed := lzfDecompress(compressed, int(ulen))
			if len(decompressed) != int(ulen) {
				return nil, fmt.Errorf("decompressed string length %d didn't match expected length %d", len(decompressed), ulen)
			}
			return decompressed, nil
		}
	}

	str := make([]byte, length)
	_, err = io.ReadFull(p.r, str)
	return str, err
}

func (p *parse) readUint8() (uint8, error) {
	b, err := p.r.ReadByte()
	return uint8(b), err
}

func (p *parse) readUint16() (uint16, error) {
	_, err := io.ReadFull(p.r, p.intBuf[:2])
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint16(p.intBuf), nil
}

func (p *parse) readUint32() (uint32, error) {
	_, err := io.ReadFull(p.r, p.intBuf[:4])
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(p.intBuf), nil
}

func (p *parse) readUint64() (uint64, error) {
	_, err := io.ReadFull(p.r, p.intBuf)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(p.intBuf), nil
}

func (p *parse) readUint32Big() (uint32, error) {
	_, err := io.ReadFull(p.r, p.intBuf[:4])
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(p.intBuf), nil
}

// Doubles are saved as strings prefixed by an unsigned
// 8 bit integer specifying the length of the representation.
// This 8 bit integer has special values in order to specify the following
// conditions:
// 253: not a number
// 254: + inf
// 255: - inf
func (p *parse) readFloat64() (float64, error) {
	length, err := p.readUint8()
	if err != nil {
		return 0, err
	}
	switch length {
	case 253:
		return math.NaN(), nil
	case 254:
		return math.Inf(0), nil
	case 255:
		return math.Inf(-1), nil
	default:
		floatBytes := make([]byte, length)
		_, err := io.ReadFull(p.r, floatBytes)
		if err != nil {
			return 0, err
		}
		f, err := strconv.ParseFloat(string(floatBytes), 64)
		return f, err
	}

	panic("not reached")
}

func (p *parse) readLength() (uint32, bool, error) {
	b, err := p.r.ReadByte()
	if err != nil {
		return 0, false, err
	}
	// The first two bits of the first byte are used to indicate the length encoding type
	switch (b & 0xc0) >> 6 {
	case rdb6bitLen:
		// When the first two bits are 00, the next 6 bits are the length.
		return uint32(b & 0x3f), false, nil
	case rdb14bitLen:
		// When the first two bits are 01, the next 14 bits are the length.
		bb, err := p.r.ReadByte()
		if err != nil {
			return 0, false, err
		}
		return (uint32(b&0x3f) << 8) | uint32(bb), false, nil
	case rdbEncVal:
		// When the first two bits are 11, the next object is encoded. 
		// The next 6 bits indicate the encoding type.
		return uint32(b & 0x3f), true, nil
	default:
		// When the first two bits are 10, the next 6 bits are discarded.
		// The next 4 bytes are the length.
		length, err := p.readUint32Big()
		return length, false, err
	}

	panic("not reached")
}

func verifyDump(d []byte) error {
	if len(d) < 10 {
		return fmt.Errorf("rdb: invalid dump length")
	}
	version := binary.LittleEndian.Uint16(d[len(d)-10:])
	if version != 6 {
		return fmt.Errorf("rdb: invalid version %d, expecting 6", version)
	}

	if binary.LittleEndian.Uint64(d[len(d)-8:]) != crc64.Digest(d[:len(d)-8]) {
		return fmt.Errorf("rdb: invalid CRC checksum")
	}

	return nil
}

func lzfDecompress(in []byte, outlen int) []byte {
	out := make([]byte, outlen)
	for i, o := 0, 0; i < len(in); {
		ctrl := int(in[i])
		i++
		if ctrl < 32 {
			for x := 0; x <= ctrl; x++ {
				out[o] = in[i]
				i++
				o++
			}
		} else {
			length := ctrl >> 5
			if length == 7 {
				length = length + int(in[i])
				i++
			}
			ref := o - ((ctrl & 0x1f) << 8) - int(in[i]) - 1
			i++
			for x := 0; x <= length+1; x++ {
				out[o] = out[ref]
				ref++
				o++
			}
		}
	}
	return out
}
