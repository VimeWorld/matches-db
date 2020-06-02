package storage

import (
	"bytes"
	"errors"
	"io"
	"log"
	"os"
	"sync"
	"time"

	"github.com/VimeWorld/matches-db/types"
	"github.com/dgraph-io/badger/v2"
	badgerOptions "github.com/dgraph-io/badger/v2/options"
	"github.com/klauspost/compress/flate"
)

const (
	matchesMetaTypeRaw   = 0
	matchesMetaTypeFlate = 1
)

type MatchesStorage struct {
	CompressThreshold int
	WriteLocked       bool

	db   *badger.DB
	path string
}

func (s *MatchesStorage) Open(path string, truncate bool) error {
	opts := badger.DefaultOptions(path).
		WithTruncate(truncate).
		WithNumMemtables(1).
		WithNumLevelZeroTables(1).
		WithNumLevelZeroTablesStall(2).
		WithKeepL0InMemory(false).
		WithNumCompactors(1).
		WithLevelOneSize(32 << 20).
		WithValueLogLoadingMode(badgerOptions.FileIO).
		WithValueLogMaxEntries(4000000).
		WithMaxBfCacheSize(5 << 20).
		WithMaxCacheSize(2 << 20).
		WithCompression(badgerOptions.ZSTD).
		WithZSTDCompressionLevel(1).
		WithLogger(&logWrapper{log.New(os.Stderr, "badger-matches ", log.LstdFlags)})

	db, err := badger.Open(opts)
	if err != nil {
		return err
	}
	s.db = db
	s.path = path
	runBadgerGc(db, 0.5)

	return nil
}

func (s *MatchesStorage) RemoveOldMatches(deadline time.Time) (deleted int, err error) {
	return s.removeOldMatchesRecursive(uint64(deadline.Unix()*1000), 0, 0)
}

func (s *MatchesStorage) removeOldMatchesRecursive(deadline uint64, deleted, retry int) (int, error) {
	deletedNow := 0
	overrun, err := s.BigTransaction(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{
			PrefetchValues: false,
		})
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			id := byteOrder.Uint64(item.Key())
			if types.GetSnowflakeTs(id) < deadline {
				err := txn.Delete(item.KeyCopy(nil))
				if err != nil {
					return err
				}
				deletedNow++
			} else {
				return nil
			}
		}
		return nil
	}, true)

	if err == badger.ErrConflict {
		if retry >= 10 {
			return deleted, errors.New("too many conflicts")
		}
		log.Println("[Matches] Conflict. Retry", retry)
		return s.removeOldMatchesRecursive(deadline, deleted, retry+1)
	}

	deleted += deletedNow

	// Если размер транзакции слишком большой, то оно закоммитит что есть и будет еще один проход
	if overrun && err == nil {
		log.Println("[Matches] Cleanup running out of txn size. Repeating")
		return s.removeOldMatchesRecursive(deadline, deleted, 0)
	}
	return deleted, err
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

func (s *MatchesStorage) BigTransaction(fn func(txn *badger.Txn) error, update bool) (overrun bool, err error) {
	txn := s.db.NewTransaction(update)
	defer txn.Discard()

	if err := fn(txn); err != nil {
		if err == badger.ErrTxnTooBig {
			return true, txn.Commit()
		}
		return false, err
	}

	return false, txn.Commit()
}

func (s *MatchesStorage) Transaction(fn func(txn *MatchesTransaction) error) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return fn(&MatchesTransaction{
			txn:               txn,
			compressThreshold: s.CompressThreshold,
		})
	})
}

func (s *MatchesStorage) Flatten() error {
	return s.db.Flatten(3)
}

func (s *MatchesStorage) Backup() error {
	return backup(s.db, s.path+"/backups")
}

func (s *MatchesStorage) Close() error {
	return s.db.Close()
}

type MatchesTransaction struct {
	txn               *badger.Txn
	compressThreshold int
}

func (t *MatchesTransaction) Put(id uint64, data []byte, copy bool) error {
	if len(data) > t.compressThreshold {
		data, err := deflate(data)
		if err != nil {
			return err
		}
		return t.txn.SetEntry(badger.NewEntry(serializeUint64(id), data).WithMeta(matchesMetaTypeFlate))
	}
	if copy {
		data = append(data[:0:0], data...)
	}
	return t.txn.SetEntry(badger.NewEntry(serializeUint64(id), data).WithMeta(matchesMetaTypeRaw))
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
