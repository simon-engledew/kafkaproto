package kafkaproto

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
)

var ErrTrailingBytes = errors.New("kafkaproto: trailing bytes after decode")

type Reader struct {
	buf []byte
	pos int
}

func NewReader(buf []byte) *Reader {
	return &Reader{buf: buf}
}

func (r *Reader) Remaining() int { return len(r.buf) - r.pos }
func (r *Reader) Pos() int       { return r.pos }

func (r *Reader) ensure(n int) error {
	if r.pos+n > len(r.buf) {
		return io.ErrUnexpectedEOF
	}
	return nil
}

func (r *Reader) ReadBool() (bool, error) {
	if err := r.ensure(1); err != nil {
		return false, err
	}
	b := r.buf[r.pos]
	r.pos++
	return b != 0, nil
}

func (r *Reader) ReadInt8() (int8, error) {
	if err := r.ensure(1); err != nil {
		return 0, err
	}
	v := int8(r.buf[r.pos])
	r.pos++
	return v, nil
}

func (r *Reader) ReadInt16() (int16, error) {
	if err := r.ensure(2); err != nil {
		return 0, err
	}
	v := int16(binary.BigEndian.Uint16(r.buf[r.pos:]))
	r.pos += 2
	return v, nil
}

func (r *Reader) ReadUint16() (uint16, error) {
	if err := r.ensure(2); err != nil {
		return 0, err
	}
	v := binary.BigEndian.Uint16(r.buf[r.pos:])
	r.pos += 2
	return v, nil
}

func (r *Reader) ReadInt32() (int32, error) {
	if err := r.ensure(4); err != nil {
		return 0, err
	}
	v := int32(binary.BigEndian.Uint32(r.buf[r.pos:]))
	r.pos += 4
	return v, nil
}

func (r *Reader) ReadUint32() (uint32, error) {
	if err := r.ensure(4); err != nil {
		return 0, err
	}
	v := binary.BigEndian.Uint32(r.buf[r.pos:])
	r.pos += 4
	return v, nil
}

func (r *Reader) ReadInt64() (int64, error) {
	if err := r.ensure(8); err != nil {
		return 0, err
	}
	v := int64(binary.BigEndian.Uint64(r.buf[r.pos:]))
	r.pos += 8
	return v, nil
}

func (r *Reader) ReadFloat64() (float64, error) {
	if err := r.ensure(8); err != nil {
		return 0, err
	}
	v := math.Float64frombits(binary.BigEndian.Uint64(r.buf[r.pos:]))
	r.pos += 8
	return v, nil
}

func (r *Reader) ReadUUID() ([16]byte, error) {
	var out [16]byte
	if err := r.ensure(16); err != nil {
		return out, err
	}
	copy(out[:], r.buf[r.pos:r.pos+16])
	r.pos += 16
	return out, nil
}

// ReadString reads a STRING (int16 length, never negative).
func (r *Reader) ReadString() (string, error) {
	n, err := r.ReadInt16()
	if err != nil {
		return "", err
	}
	if n < 0 {
		return "", errors.New("kafkaproto: unexpected null string")
	}
	if err := r.ensure(int(n)); err != nil {
		return "", err
	}
	s := string(r.buf[r.pos : r.pos+int(n)])
	r.pos += int(n)
	return s, nil
}

// ReadNullableString reads a NULLABLE_STRING (int16 length, -1 = null).
func (r *Reader) ReadNullableString() (*string, error) {
	n, err := r.ReadInt16()
	if err != nil {
		return nil, err
	}
	if n < 0 {
		return nil, nil
	}
	if err := r.ensure(int(n)); err != nil {
		return nil, err
	}
	s := string(r.buf[r.pos : r.pos+int(n)])
	r.pos += int(n)
	return &s, nil
}

// ReadCompactString reads a COMPACT_STRING (uvarint(len+1)).
func (r *Reader) ReadCompactString() (string, error) {
	n, err := r.ReadUvarint()
	if err != nil {
		return "", err
	}
	if n == 0 {
		return "", errors.New("kafkaproto: unexpected null compact string")
	}
	sz := int(n - 1)
	if err := r.ensure(sz); err != nil {
		return "", err
	}
	s := string(r.buf[r.pos : r.pos+sz])
	r.pos += sz
	return s, nil
}

// ReadCompactNullableString reads a COMPACT_NULLABLE_STRING (uvarint(len+1), 0 = null).
func (r *Reader) ReadCompactNullableString() (*string, error) {
	n, err := r.ReadUvarint()
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, nil
	}
	sz := int(n - 1)
	if err := r.ensure(sz); err != nil {
		return nil, err
	}
	s := string(r.buf[r.pos : r.pos+sz])
	r.pos += sz
	return &s, nil
}

// ReadBytes reads BYTES (int32 length, -1 = null → returns nil).
func (r *Reader) ReadBytes() ([]byte, error) {
	n, err := r.ReadInt32()
	if err != nil {
		return nil, err
	}
	if n < 0 {
		return nil, nil
	}
	if err := r.ensure(int(n)); err != nil {
		return nil, err
	}
	out := make([]byte, n)
	copy(out, r.buf[r.pos:r.pos+int(n)])
	r.pos += int(n)
	return out, nil
}

// ReadCompactBytes reads COMPACT_BYTES (uvarint(len+1), 0 = null → returns nil).
func (r *Reader) ReadCompactBytes() ([]byte, error) {
	n, err := r.ReadUvarint()
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, nil
	}
	sz := int(n - 1)
	if err := r.ensure(sz); err != nil {
		return nil, err
	}
	out := make([]byte, sz)
	copy(out, r.buf[r.pos:r.pos+sz])
	r.pos += sz
	return out, nil
}

// ReadArrayLen reads an ARRAY length (int32, -1 = null → returns -1).
func (r *Reader) ReadArrayLen() (int, error) {
	n, err := r.ReadInt32()
	if err != nil {
		return 0, err
	}
	return int(n), nil
}

// ReadCompactArrayLen reads a COMPACT_ARRAY length (uvarint(len+1), 0 = null → returns -1).
func (r *Reader) ReadCompactArrayLen() (int, error) {
	n, err := r.ReadUvarint()
	if err != nil {
		return 0, err
	}
	if n == 0 {
		return -1, nil
	}
	return int(n - 1), nil
}

func (r *Reader) ReadUvarint() (uint64, error) {
	v, n := binary.Uvarint(r.buf[r.pos:])
	if n <= 0 {
		return 0, io.ErrUnexpectedEOF
	}
	r.pos += n
	return v, nil
}

func (r *Reader) ReadVarint() (int64, error) {
	v, n := binary.Varint(r.buf[r.pos:])
	if n <= 0 {
		return 0, io.ErrUnexpectedEOF
	}
	r.pos += n
	return v, nil
}

func (r *Reader) Skip(n int) error {
	if err := r.ensure(n); err != nil {
		return err
	}
	r.pos += n
	return nil
}

// Sub returns a sub-reader over the next n bytes and advances the parent.
func (r *Reader) Sub(n int) (*Reader, error) {
	if err := r.ensure(n); err != nil {
		return nil, err
	}
	sub := &Reader{buf: r.buf[r.pos : r.pos+n]}
	r.pos += n
	return sub, nil
}

// ReadTaggedFields consumes the tagged fields block at the end of a flexible
// struct. The handler is called once per tag with a sub-reader bounded to the
// tag body. If handler is nil or returns nil for a tag it does not handle,
// any unread bytes in the sub-reader are silently discarded (parent already
// advanced). Returning an error from the handler aborts.
func (r *Reader) ReadTaggedFields(handler func(tag uint64, sub *Reader) error) error {
	count, err := r.ReadUvarint()
	if err != nil {
		return fmt.Errorf("tagged-fields count: %w", err)
	}
	for i := uint64(0); i < count; i++ {
		tag, err := r.ReadUvarint()
		if err != nil {
			return fmt.Errorf("tagged-field[%d] tag: %w", i, err)
		}
		size, err := r.ReadUvarint()
		if err != nil {
			return fmt.Errorf("tagged-field[%d] size: %w", i, err)
		}
		sub, err := r.Sub(int(size))
		if err != nil {
			return fmt.Errorf("tagged-field[%d] body: %w", i, err)
		}
		if handler != nil {
			if err := handler(tag, sub); err != nil {
				return fmt.Errorf("tagged-field[%d]: %w", i, err)
			}
		}
	}
	return nil
}
