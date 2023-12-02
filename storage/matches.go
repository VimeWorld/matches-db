package storage

import (
	"bytes"
	"io"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/klauspost/compress/flate"
)

const (
	matchesMetaTypeRaw   = byte(0)
	matchesMetaTypeFlate = byte(1)
)

type MatchesStorage struct {
	TTL         time.Duration
	WriteLocked bool

	DB *badger.DB
}

func (s *MatchesStorage) Get(id uint64) ([]byte, error) {
	var data []byte
	err := s.DB.View(func(txn *badger.Txn) error {
		item, err := txn.Get(serializeUint64(id))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err == nil {
			switch item.UserMeta() {
			case matchesMetaTypeFlate:
				err = item.Value(func(val []byte) error {
					data, err = inflate(val)
					return err
				})
			case matchesMetaTypeRaw:
				data, err = item.ValueCopy(nil)
			}
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (s *MatchesStorage) Transaction(fn func(txn *MatchesTransaction) error) error {
	return s.DB.Update(func(txn *badger.Txn) error {
		return fn(&MatchesTransaction{
			txn: txn,
			s:   s,
		})
	})
}

type MatchesTransaction struct {
	txn *badger.Txn
	s   *MatchesStorage
}

func (t *MatchesTransaction) Put(id uint64, data []byte, copy bool) error {
	meta := matchesMetaTypeRaw
	// Все что хранится в LSM сжимается автоматически
	if len(data) > int(t.s.DB.Opts().ValueThreshold) {
		var err error
		if data, err = deflate(data); err != nil {
			return err
		}
		meta = matchesMetaTypeFlate
	} else if copy {
		var c []byte
		data = append(c, data...)
	}
	return t.txn.SetEntry(
		badger.NewEntry(serializeUint64(id), data).
			WithTTL(t.s.TTL).
			WithMeta(meta),
	)
}

var deflaters = sync.Pool{New: func() interface{} {
	w, _ := flate.NewWriter(nil, -1)
	return w
}}
var inflaters = sync.Pool{New: func() interface{} {
	return flate.NewReader(nil)
}}

func deflate(data []byte) ([]byte, error) {
	compressed := &bytes.Buffer{}
	writer := deflaters.Get().(*flate.Writer)
	defer deflaters.Put(writer)
	writer.Reset(compressed)
	_, err := writer.Write(data)
	if err != nil {
		return nil, err
	}
	err = writer.Close()
	if err != nil {
		return nil, err
	}
	return compressed.Bytes(), nil
}

func inflate(data []byte) ([]byte, error) {
	uncompressed := &bytes.Buffer{}
	reader := inflaters.Get().(io.ReadCloser)
	defer inflaters.Put(reader)
	input := bytes.NewReader(data)
	err := reader.(flate.Resetter).Reset(input, nil)
	if err != nil {
		return nil, err
	}
	_, err = io.Copy(uncompressed, reader)
	input.Reset(nil)
	if err != nil {
		return nil, err
	}
	err = reader.Close()
	if err != nil {
		return nil, err
	}
	return uncompressed.Bytes(), nil
}
