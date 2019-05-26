package storage

import (
	"log"
	"os"
	"time"

	"github.com/VimeWorld/matches-db/types"
	"github.com/dgraph-io/badger"
)

const (
	keyLength    = 4
	bucketLength = 4
)

var userMatchesDescriptor = &valueDescriptor{
	version:  1,
	size:     matchSize,
	migrator: migrateMatches,
}

var bucketsDescriptor = &valueDescriptor{
	version: 1,
	size:    bucketLength,
}

type UserStorage struct {
	db *badger.DB
}

func (s *UserStorage) Open(path string, truncate bool) error {
	opts := badger.DefaultOptions
	opts.Dir = path
	opts.ValueDir = path
	opts.Truncate = truncate
	opts.ValueThreshold = 32
	opts.ValueLogFileSize = 128 << 20
	opts.MaxTableSize = 32 << 20
	opts.NumMemtables = 2
	opts.LevelOneSize = 32 << 20
	opts.NumLevelZeroTables = 1
	opts.NumLevelZeroTablesStall = 2
	opts.Logger = &logWrapper{log.New(os.Stderr, "badger-users ", log.LstdFlags)}

	db, err := badger.Open(opts)
	if err != nil {
		return err
	}
	s.db = db
	runBadgerGc(db, 0.5)

	return nil
}

func (s *UserStorage) GetLastUserMatches(id uint32, count int) ([]*types.UserMatch, error) {
	var matches []*types.UserMatch
	err := s.Transaction(func(txn *UsersTransaction) error {
		m, err := txn.GetLastUserMatches(id, count)
		matches = m
		return err
	}, false)
	return matches, err
}

func (s *UserStorage) RemoveOldMatches(deadline time.Time) (deleted int, err error) {
	return s.removeOldMatchesRecursive(deadline, deleted)
}

func (s *UserStorage) removeOldMatchesRecursive(deadline time.Time, deleted int) (int, error) {
	deadlineMillis := uint64(deadline.Unix() * 1000)
	maxBucketNumber := getBucketNumber(time.Duration(deadlineMillis) * time.Millisecond)
	overrun, err := s.BigTransaction(func(txn *UsersTransaction) error {
		btxn := txn.txn
		it := btxn.NewIterator(badger.IteratorOptions{
			PrefetchValues: false,
		})
		defer it.Close()

		match := &types.UserMatch{}
		buffer := newByteBuf(nil, false)

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()

			if len(item.Key()) != keyLength+bucketLength {
				continue
			}
			bucket := item.Key()[keyLength:]
			bucketNumber := byteOrder.Uint32(bucket)

			// Сначала чистится список ведер, а только затем удаляется само ведро.
			// В противном случае, ведро может удалиться, а в списке ведер оно останется навсегда, утечка.

			// Номер ведра меньше актуального, можно удалить его и все что в нем лежит.
			if bucketNumber < maxBucketNumber {
				key := item.KeyCopy(nil)
				err := removeValue(btxn, key[:keyLength], bucket, false, bucketsDescriptor)
				if err != nil {
					return err
				}
				err = btxn.Delete(key)
				if err != nil {
					return err
				}
				continue
			}

			// Номер ведра равен последнему, а значит в нем лежат матчи, которые нужно отфильтровать
			if bucketNumber == maxBucketNumber {
				ver := item.UserMeta()
				err := item.Value(func(val []byte) error {
					buffer.Reset(val, false)
					for buffer.Remaining() > 0 {
						err := readMatch(ver, buffer, match)
						if err != nil {
							return err
						}
						if types.GetSnowflakeTs(match.Id) >= deadlineMillis {
							break
						} else {
							deleted++
						}
					}
					if buffer.Remaining() == 0 {
						key := item.KeyCopy(nil)
						err := removeValue(btxn, key[:keyLength], bucket, false, bucketsDescriptor)
						if err != nil {
							return err
						}
						return btxn.Delete(key)
					}
					if buffer.readerIndex > 0 {
						copiedVal := append(val[:0:0], val[buffer.readerIndex:]...)
						return btxn.SetWithMeta(item.KeyCopy(nil), copiedVal, ver)
					}
					return nil
				})
				if err != nil {
					return err
				}
			}
		}
		return nil
	}, true)

	// Если размер транзакции слишком большой, то оно закоммитит что есть и будет еще один проход
	if overrun && err == nil {
		log.Println("Cleanup running out of txn size. Repeating")
		return s.removeOldMatchesRecursive(deadline, deleted)
	}
	return deleted, err
}

func (s *UserStorage) Transaction(fn func(txn *UsersTransaction) error, update bool) error {
	return s.db.Update(func(txn *badger.Txn) error {
		userTxn := &UsersTransaction{
			txn: txn,
		}
		if err := fn(userTxn); err != nil {
			return err
		}
		return nil
	})
}

func (s *UserStorage) BigTransaction(fn func(txn *UsersTransaction) error, update bool) (overrun bool, err error) {
	txn := s.db.NewTransaction(update)
	defer txn.Discard()

	userTxn := &UsersTransaction{
		txn: txn,
	}
	if err := fn(userTxn); err != nil {
		if err == badger.ErrTxnTooBig {
			return true, txn.Commit()
		}
		return false, err
	}

	return false, txn.Commit()
}

func (s *UserStorage) Close() error {
	return s.db.Close()
}

type UsersTransaction struct {
	txn *badger.Txn
}

func (t *UsersTransaction) AddMatch(userid uint32, matchid uint64, win bool) error {
	value := serializeMatch(matchid, win)
	bucket := getBucket(matchid)
	userBytes := serializeUint32(userid)
	err := appendValueIfNotExists(t.txn, userBytes, bucket, bucketsDescriptor)
	if err != nil {
		return err
	}
	key := make([]byte, 8)
	copy(key, userBytes)
	copy(key[4:], bucket)
	return appendValue(t.txn, key, value, userMatchesDescriptor)
}

func (t *UsersTransaction) GetLastUserMatches(userid uint32, count int) ([]*types.UserMatch, error) {
	key := serializeUint32(userid)
	var matches []*types.UserMatch
	buckets, err := t.getBuckets(key)
	if err != nil {
		return matches, err
	}
	k := make([]byte, keyLength+bucketLength)
	copy(k, key)
	for i := len(buckets) - 1; i >= 0; i-- {
		copy(k[keyLength:], buckets[i])
		temp, err := t.getMatches(k)
		if err != nil {
			return matches, err
		}
		if len(temp) >= count && matches == nil {
			matches = temp
			break
		}
		matches = append(temp, matches...)
		if len(matches) >= count {
			break
		}
	}
	return matches, nil
}

func (t *UsersTransaction) getBuckets(key []byte) ([][]byte, error) {
	value, _, err := getWithValue(t.txn, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	index := make([][]byte, len(value)/bucketLength)
	for i := range index {
		index[i] = value[i*bucketLength : (i+1)*bucketLength]
	}
	return index, nil
}

func (t *UsersTransaction) getMatches(bucket []byte) ([]*types.UserMatch, error) {
	value, version, err := getWithValue(t.txn, bucket)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return readMatches(version, value)
}

func migrateMatches(old []byte, version byte) ([]byte, error) {
	matches, err := readMatches(version, old)
	if err != nil {
		return nil, err
	}
	return writeMatches(matches)
}

func getBucket(matchid uint64) []byte {
	d := time.Duration(types.GetSnowflakeTs(matchid)) * time.Millisecond
	return serializeUint32(getBucketNumber(d))
}

func getBucketNumber(d time.Duration) uint32 {
	return uint32(d.Hours() / 24 / 10)
}
