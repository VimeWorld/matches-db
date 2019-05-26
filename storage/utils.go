package storage

import (
	"bytes"
	"log"
	"time"

	"github.com/dgraph-io/badger"
)

type valueDescriptor struct {
	version  byte
	size     int
	migrator func(old []byte, version byte) ([]byte, error)
}

// Получает текущее значение по ключу key и добавляет в его конец appendix.
//
// Если такого ключа не существует, то сохраняется только appendix.
//
// Если сохраненная версия не соответствует текущей, то будет вызван метод миграции из дескриптора,
// а только затем добавлено и записано новое значение.
func appendValue(txn *badger.Txn, key, appendix []byte, config *valueDescriptor) error {
	stored, version, err := getWithValue(txn, key)

	if err == badger.ErrKeyNotFound {
		return txn.SetWithMeta(key, appendix, config.version)
	} else if err != nil {
		return err
	}

	if version != config.version {
		fixed, err := config.migrator(stored, version)
		if err != nil {
			return err
		}
		newValue := make([]byte, len(fixed)+len(appendix))
		copy(newValue, fixed)
		copy(newValue[len(fixed):], appendix)
		return txn.SetWithMeta(key, newValue, config.version)
	}

	newValue := make([]byte, len(stored)+len(appendix))
	copy(newValue, stored)
	copy(newValue[len(stored):], appendix)
	return txn.SetWithMeta(key, newValue, config.version)
}

// Метод аналогичен appendValue, за исключением того что сохраненные данные воспринимаются
// в качестве Set и в них не могут содержаться одинаковые значения.
//
// На одинаковость значения проверяются через сравнение кусков байт фиксированной длины из дескриптора.
func appendValueIfNotExists(txn *badger.Txn, key, appendix []byte, config *valueDescriptor) error {
	stored, version, err := getWithValue(txn, key)

	if err == badger.ErrKeyNotFound {
		return txn.SetWithMeta(key, appendix, config.version)
	} else if err != nil {
		return err
	}

	updated := false
	if version != config.version {
		fixed, err := config.migrator(stored, version)
		stored = fixed
		if err != nil {
			return err
		}
		updated = true
	}

	exists := false
	size := config.size
	for i := len(stored)/size - 1; i >= 0; i-- {
		if bytes.Equal(stored[i*size:(i+1)*size], appendix) {
			exists = true
			break
		}
	}

	if !exists {
		updated = true
		newValue := make([]byte, len(stored)+len(appendix))
		copy(newValue, stored)
		copy(newValue[len(stored):], appendix)
		stored = newValue
	}

	if updated {
		return txn.SetWithMeta(key, stored, config.version)
	}
	return nil
}

func removeValue(txn *badger.Txn, key, value []byte, multiple bool, config *valueDescriptor) error {
	stored, version, err := getWithValue(txn, key)
	if err == badger.ErrKeyNotFound {
		return nil
	} else if err != nil {
		return err
	}

	updated := false
	if version != config.version {
		fixed, err := config.migrator(stored, version)
		stored = fixed
		if err != nil {
			return err
		}
		updated = true
	}

	size := config.size
	for i := len(stored)/size - 1; i >= 0; i-- {
		if bytes.Equal(stored[i*size:(i+1)*size], value) {
			stored = append(stored[:i*size], stored[(i+1)*size:]...)
			updated = true
			if multiple {
				continue
			} else {
				break
			}
		}
	}
	if len(stored) == 0 {
		return txn.Delete(key)
	}
	if updated {
		return txn.SetWithMeta(key, stored, config.version)
	}
	return nil
}

func getWithValue(txn *badger.Txn, key []byte) (value []byte, version byte, err error) {
	item, err := txn.Get(key)
	if err != nil {
		return nil, 0, err
	}
	value, err = item.ValueCopy(nil)
	if err != nil {
		return nil, 0, err
	}
	return value, item.UserMeta(), nil
}

type logWrapper struct {
	*log.Logger
}

func (l *logWrapper) Errorf(f string, v ...interface{}) {
	l.Printf("ERROR: "+f, v...)
}

func (l *logWrapper) Warningf(f string, v ...interface{}) {
	l.Printf("WARNING: "+f, v...)
}

func (l *logWrapper) Infof(f string, v ...interface{}) {
	l.Printf("INFO: "+f, v...)
}

func (l *logWrapper) Debugf(f string, v ...interface{}) {
	l.Printf("DEBUG: "+f, v...)
}

func runBadgerGc(db *badger.DB, discardRatio float64) {
	go func() {
		for range time.Tick(5 * time.Minute) {
			for {
				err := db.RunValueLogGC(discardRatio)
				if err != nil {
					break
				}
			}
		}
	}()
}
