package storage

import (
	"errors"
	"log"
	"os"
	"sort"
	"time"

	"github.com/VimeWorld/matches-db/types"
	"github.com/dgraph-io/badger/v2"
	badgerOptions "github.com/dgraph-io/badger/v2/options"
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
	db   *badger.DB
	path string
}

func (s *UserStorage) Open(path string, truncate bool) error {
	opts := badger.DefaultOptions(path).
		WithTruncate(truncate).
		WithNumMemtables(1).
		WithNumLevelZeroTables(1).
		WithNumLevelZeroTablesStall(2).
		WithKeepL0InMemory(false).
		WithNumCompactors(1).
		WithLevelOneSize(32 << 20).
		WithValueLogFileSize(128 << 20).
		WithValueLogLoadingMode(badgerOptions.FileIO).
		WithMaxBfCacheSize(5 << 20).
		WithMaxCacheSize(2 << 20).
		WithCompression(badgerOptions.ZSTD).
		WithZSTDCompressionLevel(1).
		WithLogger(&logWrapper{log.New(os.Stderr, "badger-users ", log.LstdFlags)})

	db, err := badger.Open(opts)
	if err != nil {
		return err
	}
	s.db = db
	s.path = path
	runBadgerGc(db, 0.5)

	return nil
}

func (s *UserStorage) GetLastUserMatches(id uint32, offset, count int) ([]*types.UserMatch, error) {
	var matches []*types.UserMatch
	err := s.Transaction(func(txn *UsersTransaction) error {
		m, err := txn.GetLastUserMatches(id, offset, count)
		matches = m
		return err
	}, false)
	return matches, err
}

func (s *UserStorage) GetUserMatchesAfter(id uint32, begin uint64, count int) ([]*types.UserMatch, error) {
	var matches []*types.UserMatch
	err := s.Transaction(func(txn *UsersTransaction) error {
		m, err := txn.GetUserMatchesAfter(id, begin, count)
		matches = m
		return err
	}, false)
	return matches, err
}

func (s *UserStorage) GetUserMatchesBefore(id uint32, begin uint64, count int) ([]*types.UserMatch, error) {
	var matches []*types.UserMatch
	err := s.Transaction(func(txn *UsersTransaction) error {
		m, err := txn.GetUserMatchesBefore(id, begin, count)
		matches = m
		return err
	}, false)
	return matches, err
}

func (s *UserStorage) RemoveOldMatches(deadline time.Time) (deleted int, err error) {
	return s.removeOldMatchesRecursive(deadline, deleted, 0)
}

func (s *UserStorage) removeOldMatchesRecursive(deadline time.Time, deleted, retry int) (int, error) {
	deadlineMillis := uint64(deadline.Unix() * 1000)
	maxBucketNumber := getBucketNumberFromMillis(time.Duration(deadlineMillis) * time.Millisecond)
	deletedNow := 0
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

			if item.KeySize() != keyLength+bucketLength {
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
				deletedNow++
				continue
			}

			// Номер ведра равен последнему, а значит в нем лежат матчи, которые нужно отфильтровать
			if bucketNumber == maxBucketNumber {
				changed := false
				ver := item.UserMeta()
				err := item.Value(func(val []byte) error {
					buffer.Reset(val, false)
					// Удаление матчей из ведра
					for buffer.Remaining() > 0 {
						err := readMatch(ver, buffer, match)
						if err != nil {
							return err
						}
						if types.GetSnowflakeTs(match.Id) >= deadlineMillis {
							break
						} else {
							changed = true
						}
					}
					// Если ведро осталось пустым
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
						return btxn.SetEntry(badger.NewEntry(item.KeyCopy(nil), copiedVal).WithMeta(ver))
					}
					return nil
				})
				if err != nil {
					return err
				}
				if changed {
					deletedNow++
				}
			}
			if deletedNow > 5000 {
				// Форсим коммит, чтобы была меньше вероятность конфликтов
				return badger.ErrTxnTooBig
			}
		}
		return nil
	}, true)

	if err == badger.ErrConflict {
		if retry >= 10 {
			return deleted, errors.New("too many conflicts")
		}
		log.Println("[Users] Conflict. Retry", retry)
		return s.removeOldMatchesRecursive(deadline, deleted, retry+1)
	}

	deleted += deletedNow

	// Если размер транзакции слишком большой, то оно закоммитит что есть и будет еще один проход
	if overrun && err == nil {
		log.Println("[Users] Cleanup running out of txn size. Repeating")
		return s.removeOldMatchesRecursive(deadline, deleted, 0)
	}
	return deleted, err
}

func (s *UserStorage) Transaction(fn func(txn *UsersTransaction) error, update bool) error {
	cb := func(txn *badger.Txn) error {
		userTxn := &UsersTransaction{
			txn: txn,
		}
		if err := fn(userTxn); err != nil {
			return err
		}
		return nil
	}
	if update {
		return s.db.Update(cb)
	} else {
		return s.db.View(cb)
	}
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

func (s *UserStorage) Flatten() error {
	return s.db.Flatten(3)
}

func (s *UserStorage) Backup() error {
	return backup(s.db, s.path+"/backups")
}

func (s *UserStorage) Close() error {
	return s.db.Close()
}

type UsersTransaction struct {
	txn *badger.Txn
}

func (t *UsersTransaction) AddMatch(userid uint32, matchid uint64, state byte) error {
	value := serializeMatch(matchid, state)
	bucket := serializeUint32(getBucketNumberFromId(matchid))
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

func (t *UsersTransaction) GetLastUserMatches(userid uint32, offset, count int) ([]*types.UserMatch, error) {
	key := serializeUint32(userid)
	var matches []*types.UserMatch
	buckets, err := t.getBuckets(key)
	if err != nil {
		return matches, err
	}
	offsetBytes := offset * matchSize
	remainingBytes := count * matchSize
	k := make([]byte, keyLength+bucketLength)
	copy(k, key)
	// search in reverse order
	for i := len(buckets) - 1; i >= 0; i-- {
		copy(k[keyLength:], buckets[i])
		value, version, err := getWithValue(t.txn, k)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				continue
			}
			return matches, err
		}

		// Пропускаем все матчи без их считывания
		if offsetBytes > 0 {
			if len(value) <= offsetBytes {
				offsetBytes -= len(value)
				continue
			}
			value = value[:len(value)-offsetBytes]
			offsetBytes = 0
		}

		// Чтобы не читать лишнего, ограничиваем
		if len(value) > remainingBytes {
			value = value[len(value)-remainingBytes:]
		} else {
			remainingBytes -= len(value)
		}

		temp, err := readMatches(version, value)
		if err != nil {
			return matches, err
		}
		matches = append(temp, matches...)
		if len(matches) >= count {
			break
		}
	}
	return matches, nil
}

func (t *UsersTransaction) GetUserMatchesAfter(userid uint32, begin uint64, count int) ([]*types.UserMatch, error) {
	key := serializeUint32(userid)
	var matches []*types.UserMatch
	buckets, err := t.getBuckets(key)
	if err != nil {
		return matches, err
	}
	k := make([]byte, keyLength+bucketLength)
	copy(k, key)
	fromBucket := getBucketNumberFromId(begin)
	for i := 0; i < len(buckets); i++ {
		currentBucket := byteOrder.Uint32(buckets[i])
		if currentBucket < fromBucket {
			continue
		}

		copy(k[keyLength:], buckets[i])
		value, version, err := getWithValue(t.txn, k)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				continue
			}
			return matches, err
		}

		idxFrom := sort.Search(len(value)/matchSize, func(idx int) bool {
			id := byteOrder.Uint64(value[idx*matchSize : (idx+1)*matchSize])
			return id > begin
		})

		idxTo := len(value) / matchSize
		if idxTo-idxFrom > count-len(matches) {
			idxTo = idxFrom + count - len(matches)
		}
		if idxTo == idxFrom {
			continue
		}

		temp, err := readMatches(version, value[idxFrom*matchSize:idxTo*matchSize])
		if err != nil {
			return matches, err
		}

		matches = append(matches, temp...)
		if len(matches) >= count {
			break
		}
	}
	return matches, nil
}

func (t *UsersTransaction) GetUserMatchesBefore(userid uint32, begin uint64, count int) ([]*types.UserMatch, error) {
	key := serializeUint32(userid)
	var matches []*types.UserMatch
	buckets, err := t.getBuckets(key)
	if err != nil {
		return matches, err
	}
	k := make([]byte, keyLength+bucketLength)
	copy(k, key)
	fromBucket := getBucketNumberFromId(begin)

	// search in reverse order
	for i := len(buckets) - 1; i >= 0; i-- {
		currentBucket := byteOrder.Uint32(buckets[i])
		if currentBucket > fromBucket {
			continue
		}

		copy(k[keyLength:], buckets[i])
		value, version, err := getWithValue(t.txn, k)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				continue
			}
			return matches, err
		}

		idxTo := sort.Search(len(value)/matchSize, func(idx int) bool {
			id := byteOrder.Uint64(value[idx*matchSize : (idx+1)*matchSize])
			return id >= begin
		})
		if idxTo == 0 {
			continue
		}

		idxFrom := 0
		if idxTo > count-len(matches) {
			idxFrom = idxTo - (count - len(matches))
		}

		temp, err := readMatches(version, value[idxFrom*matchSize:idxTo*matchSize])
		if err != nil {
			return matches, err
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

func getBucketNumberFromId(matchid uint64) uint32 {
	return getBucketNumberFromMillis(time.Duration(types.GetSnowflakeTs(matchid)) * time.Millisecond)
}

func getBucketNumberFromMillis(d time.Duration) uint32 {
	return uint32(d.Hours() / 24 / 10)
}
