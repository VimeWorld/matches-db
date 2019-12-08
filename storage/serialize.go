package storage

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/VimeWorld/matches-db/types"
)

const (
	matchSize = 9
)

var byteOrder = binary.BigEndian

type byteBuf struct {
	buf         []byte
	writerIndex int
	readerIndex int
	grow        bool
}

func newByteBuf(buf []byte, grow bool) *byteBuf {
	return &byteBuf{
		buf:  buf,
		grow: grow,
	}
}

func (b *byteBuf) Reset(buf []byte, grow bool) {
	b.buf = buf
	b.writerIndex = 0
	b.readerIndex = 0
	b.grow = grow
}

func (b *byteBuf) Write(p []byte) {
	if b.grow {
		b.buf = append(b.buf, p...)
		b.writerIndex += len(p)
	} else {
		b.writerIndex += copy(b.buf[b.writerIndex:], p)
	}
}

func (b *byteBuf) WriteByte(p byte) {
	if b.grow {
		b.buf = append(b.buf, p)
	} else {
		b.buf[b.writerIndex] = p
	}
	b.writerIndex++
}

func (b *byteBuf) WriteUint64(p uint64) {
	buf := make([]byte, 8)
	byteOrder.PutUint64(buf, p)
	b.Write(buf)
}

func (b *byteBuf) WriteInt64(p int64) {
	b.WriteUint64(uint64(p))
}

func (b *byteBuf) WriteUint32(p uint32) {
	buf := make([]byte, 4)
	byteOrder.PutUint32(buf, p)
	b.Write(buf)
}

func (b *byteBuf) WriteInt32(p int32) {
	b.WriteUint32(uint32(p))
}

func (b *byteBuf) WriteBool(p bool) {
	if p {
		b.WriteByte(1)
	} else {
		b.WriteByte(0)
	}
}

func (b *byteBuf) Read(count int) []byte {
	slice := b.buf[b.readerIndex : b.readerIndex+count]
	b.readerIndex += count
	return slice
}

func (b *byteBuf) ReadByte() byte {
	b.readerIndex++
	return b.buf[b.readerIndex-1]
}

func (b *byteBuf) ReadUint64() uint64 {
	return byteOrder.Uint64(b.Read(8))
}

func (b *byteBuf) ReadInt64() int64 {
	return int64(b.ReadUint64())
}

func (b *byteBuf) ReadUint32() uint32 {
	return byteOrder.Uint32(b.Read(4))
}

func (b *byteBuf) ReadInt32() int32 {
	return int32(b.ReadUint32())
}

func (b *byteBuf) ReadBool() bool {
	return b.ReadByte() == 1
}

func (b *byteBuf) Remaining() int {
	return len(b.buf) - b.readerIndex
}

func readMatches(version byte, buf []byte) ([]*types.UserMatch, error) {
	if version == 1 {
		buffer := newByteBuf(buf, false)
		matches := make([]*types.UserMatch, len(buf)/matchSize)
		index := 0
		for buffer.Remaining() > 0 {
			m := &types.UserMatch{}
			m.Id = buffer.ReadUint64()
			m.State = buffer.ReadByte()
			matches[index] = m
			index++
		}
		return matches, nil
	}
	return nil, errors.New(fmt.Sprint("unsupported version", version))
}

func readMatch(version byte, reader *byteBuf, match *types.UserMatch) error {
	if version == 1 {
		match.Id = reader.ReadUint64()
		match.State = reader.ReadByte()
		return nil
	}
	return errors.New(fmt.Sprint("unsupported version", version))
}

func writeMatches(matches []*types.UserMatch) ([]byte, error) {
	buffer := newByteBuf(make([]byte, len(matches)*matchSize), false)
	for _, match := range matches {
		writeMatch(buffer, match)
	}
	return buffer.buf, nil
}

func writeMatch(writer *byteBuf, match *types.UserMatch) {
	writer.WriteUint64(match.Id)
	writer.WriteByte(match.State)
}

func serializeMatch(id uint64, state byte) []byte {
	buf := newByteBuf(make([]byte, matchSize), false)
	buf.WriteUint64(id)
	buf.WriteByte(state)
	return buf.buf
}

func serializeUint32(num uint32) []byte {
	b := make([]byte, 4)
	byteOrder.PutUint32(b, num)
	return b
}

func serializeUint64(num uint64) []byte {
	b := make([]byte, 8)
	byteOrder.PutUint64(b, num)
	return b
}
