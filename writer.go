package kafkaproto

import (
	"encoding/binary"
	"math"
)

// Writer is the encoding counterpart of Reader. It accumulates wire bytes in
// an internal buffer; callers retrieve the result via Bytes().
type Writer struct {
	buf []byte
}

func NewWriter() *Writer { return &Writer{} }

func (w *Writer) Bytes() []byte { return w.buf }
func (w *Writer) Len() int      { return len(w.buf) }

func (w *Writer) WriteBool(v bool) {
	if v {
		w.buf = append(w.buf, 1)
	} else {
		w.buf = append(w.buf, 0)
	}
}

func (w *Writer) WriteInt8(v int8)     { w.buf = append(w.buf, byte(v)) }
func (w *Writer) WriteInt16(v int16)   { w.buf = binary.BigEndian.AppendUint16(w.buf, uint16(v)) }
func (w *Writer) WriteUint16(v uint16) { w.buf = binary.BigEndian.AppendUint16(w.buf, v) }
func (w *Writer) WriteInt32(v int32)   { w.buf = binary.BigEndian.AppendUint32(w.buf, uint32(v)) }
func (w *Writer) WriteUint32(v uint32) { w.buf = binary.BigEndian.AppendUint32(w.buf, v) }
func (w *Writer) WriteInt64(v int64)   { w.buf = binary.BigEndian.AppendUint64(w.buf, uint64(v)) }

func (w *Writer) WriteFloat64(v float64) {
	w.buf = binary.BigEndian.AppendUint64(w.buf, math.Float64bits(v))
}

func (w *Writer) WriteUUID(v [16]byte) { w.buf = append(w.buf, v[:]...) }

// WriteString writes a non-nullable STRING (int16 length).
func (w *Writer) WriteString(v string) {
	w.WriteInt16(int16(len(v)))
	w.buf = append(w.buf, v...)
}

// WriteNullableString writes a NULLABLE_STRING. nil → -1 length.
func (w *Writer) WriteNullableString(v *string) {
	if v == nil {
		w.WriteInt16(-1)
		return
	}
	w.WriteInt16(int16(len(*v)))
	w.buf = append(w.buf, *v...)
}

// WriteCompactString writes a COMPACT_STRING (uvarint(len+1) + bytes).
func (w *Writer) WriteCompactString(v string) {
	w.WriteUvarint(uint64(len(v)) + 1)
	w.buf = append(w.buf, v...)
}

// WriteCompactNullableString writes a COMPACT_NULLABLE_STRING. nil → 0.
func (w *Writer) WriteCompactNullableString(v *string) {
	if v == nil {
		w.WriteUvarint(0)
		return
	}
	w.WriteUvarint(uint64(len(*v)) + 1)
	w.buf = append(w.buf, *v...)
}

// WriteBytes writes BYTES. nil → -1 length.
func (w *Writer) WriteBytes(v []byte) {
	if v == nil {
		w.WriteInt32(-1)
		return
	}
	w.WriteInt32(int32(len(v)))
	w.buf = append(w.buf, v...)
}

// WriteCompactBytes writes COMPACT_BYTES. nil → 0.
func (w *Writer) WriteCompactBytes(v []byte) {
	if v == nil {
		w.WriteUvarint(0)
		return
	}
	w.WriteUvarint(uint64(len(v)) + 1)
	w.buf = append(w.buf, v...)
}

// WriteArrayLen writes a non-compact ARRAY length. n == -1 means null.
func (w *Writer) WriteArrayLen(n int) { w.WriteInt32(int32(n)) }

// WriteCompactArrayLen writes a COMPACT_ARRAY length. n == -1 means null.
func (w *Writer) WriteCompactArrayLen(n int) {
	if n < 0 {
		w.WriteUvarint(0)
		return
	}
	w.WriteUvarint(uint64(n) + 1)
}

func (w *Writer) WriteUvarint(v uint64) {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], v)
	w.buf = append(w.buf, buf[:n]...)
}

func (w *Writer) WriteVarint(v int64) {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutVarint(buf[:], v)
	w.buf = append(w.buf, buf[:n]...)
}

func (w *Writer) WriteRaw(b []byte) { w.buf = append(w.buf, b...) }

// TaggedField is a captured (tag, body) pair, used to defer writing the
// tagged-fields block until all entries are known.
type TaggedField struct {
	Tag  uint64
	Body []byte
}

// WriteTaggedFields writes the count + (tag, size, body) entries for a
// flexible-version struct's tagged-fields block. Entries are written in the
// order provided; the spec requires ascending tag order and the generator
// emits tags in that order.
func (w *Writer) WriteTaggedFields(fields []TaggedField) {
	w.WriteUvarint(uint64(len(fields)))
	for _, f := range fields {
		w.WriteUvarint(f.Tag)
		w.WriteUvarint(uint64(len(f.Body)))
		w.buf = append(w.buf, f.Body...)
	}
}
