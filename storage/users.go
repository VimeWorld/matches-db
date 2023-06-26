package storage

import (
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

type UserStorage struct {
	db   *badger.DB
	path string
	TTL  time.Duration

	userMatchesDescriptor *valueDescriptor
	bucketsDescriptor     *valueDescriptor
}

func (s *UserStorage) Open(path string, truncate, ignoreConflicts bool) error {
	opts := badger.DefaultOptions(path).
		WithTruncate(truncate).
		WithDetectConflicts(!ignoreConflicts).
		WithNumMemtables(2).
		WithNumLevelZeroTables(2).
		WithNumLevelZeroTablesStall(4).
		WithValueLogFileSize(128 << 20).
		WithIndexCacheSize(200 << 20).
		WithCompression(badgerOptions.ZSTD).
		WithZSTDCompressionLevel(1).
		WithLogger(&logWrapper{log.New(os.Stderr, "badger-users ", log.LstdFlags)})

	s.userMatchesDescriptor = &valueDescriptor{
		version:  1,
		size:     matchSize,
		migrator: migrateMatches,
		ttl:      s.TTL,
	}
	s.bucketsDescriptor = &valueDescriptor{
		version: 1,
		size:    bucketLength,
		ttl:     s.TTL + 10*24*time.Hour,
	}

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

func (s *UserStorage) Transaction(fn func(txn *UsersTransaction) error, update bool) error {
	cb := func(txn *badger.Txn) error {
		userTxn := &UsersTransaction{
			s:   s,
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

func (s *UserStorage) Flatten() error {
	return s.db.Flatten(3)
}

func (s *UserStorage) Backup() error {
	return backup(s.db, s.path+"/backups")
}

func (s *UserStorage) Close() error {
	return s.db.Close()
}

func (s *UserStorage) oldestBucketNum() uint32 {
	millis := time.Duration(time.Now().Add(-s.TTL).UnixMilli()) * time.Millisecond
	return getBucketNumberFromMillis(millis)
}

type UsersTransaction struct {
	s   *UserStorage
	txn *badger.Txn
}

func (t *UsersTransaction) AddMatch(userid uint32, matchid uint64, state byte) error {
	value := serializeMatch(matchid, state)
	bucket := serializeUint32(getBucketNumberFromId(matchid))
	userBytes := serializeUint32(userid)
	err := appendValueIfNotExistsAndFilter(t.txn, userBytes, bucket, t.filterOldBuckets, t.s.bucketsDescriptor)
	if err != nil {
		return err
	}
	key := make([]byte, 8)
	copy(key, userBytes)
	copy(key[4:], bucket)
	return appendValue(t.txn, key, value, t.s.userMatchesDescriptor)
}

func (t *UsersTransaction) filterOldBuckets(buckets []byte) []byte {
	minBucketNumber := t.s.oldestBucketNum()
	size := t.s.bucketsDescriptor.size
	for i := 0; i < len(buckets)/size; i++ {
		num := deserializeUint32(buckets[i*size : (i+1)*size])
		if num >= minBucketNumber {
			return buckets[i*size:]
		}
	}
	return buckets[:0]
}

func (t *UsersTransaction) GetLastUserMatches(userid uint32, offset, count int) ([]*types.UserMatch, error) {
	key := serializeUint32(userid)
	var matches []*types.UserMatch
	buckets, err := t.getBuckets(key)
	if err != nil {
		return nil, err
	}
	oldestBucketNum := t.s.oldestBucketNum()
	offsetBytes := offset * matchSize
	remainingBytes := count * matchSize
	k := make([]byte, keyLength+bucketLength)
	copy(k, key)
	// search in reverse order
	for i := len(buckets) - 1; i >= 0; i-- {
		currentBucket := byteOrder.Uint32(buckets[i])
		if currentBucket < oldestBucketNum {
			break
		}

		copy(k[keyLength:], buckets[i])
		value, version, err := getWithValue(t.txn, k)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				continue
			}
			return nil, err
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
			return nil, err
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
	oldestBucketNum := t.s.oldestBucketNum()
	if fromBucket < oldestBucketNum {
		fromBucket = oldestBucketNum
	}
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
	oldestBucketNum := t.s.oldestBucketNum()

	// search in reverse order
	for i := len(buckets) - 1; i >= 0; i-- {
		currentBucket := byteOrder.Uint32(buckets[i])
		if currentBucket > fromBucket {
			continue
		}
		if currentBucket < oldestBucketNum {
			break
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
