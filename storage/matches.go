package storage

import (
	"bytes"
	"github.com/dgraph-io/badger"
	"github.com/klauspost/compress/flate"
	"io"
	"log"
	"os"
	"sync"
)

const (
	matchesMetaTypeRaw   = 0
	matchesMetaTypeFlate = 1
)

type MatchesStorage struct {
	db                *badger.DB
	CompressThreshold int
}

func (s *MatchesStorage) Open(path string, truncate bool) error {
	opts := badger.DefaultOptions
	opts.Dir = path
	opts.ValueDir = path
	opts.Truncate = truncate
	opts.MaxTableSize = 6 << 20
	opts.NumMemtables = 2
	opts.LevelOneSize = 32 << 20
	opts.Logger = &logWrapper{log.New(os.Stderr, "badger-matches ", log.LstdFlags)}

	db, err := badger.Open(opts)
	if err != nil {
		return err
	}
	s.db = db
	runBadgerGc(db, 0.5)

	return nil
}

func (s *MatchesStorage) Get(id uint64) ([]byte, error) {
	var data []byte
	err := s.db.View(func(txn *badger.Txn) error {
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
	return s.db.Update(func(txn *badger.Txn) error {
		return fn(&MatchesTransaction{
			txn:               txn,
			compressThreshold: s.CompressThreshold,
		})
	})
}

func (s *MatchesStorage) Close() error {
	return s.db.Close()
}

type MatchesTransaction struct {
	txn               *badger.Txn
	compressThreshold int
}

func (t *MatchesTransaction) Put(id uint64, data []byte) error {
	if len(data) > t.compressThreshold {
		data, err := deflate(data)
		if err != nil {
			return err
		}
		return t.txn.SetWithMeta(serializeUint64(id), data, matchesMetaTypeFlate)
	}
	return t.txn.SetWithMeta(serializeUint64(id), data, matchesMetaTypeRaw)
}

var deflaters = sync.Pool{New: func() interface{} {
	w, _ := flate.NewWriter(nil, 3)
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
