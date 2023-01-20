package storage

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/dgraph-io/badger/v2"
)

type valueDescriptor struct {
	version  byte
	size     int
	ttl      time.Duration
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
		return txn.SetEntry(
			badger.NewEntry(key, appendix).
				WithMeta(config.version).
				WithTTL(config.ttl),
		)
	} else if err != nil {
		return err
	}

	if version != config.version {
		fixed, err := config.migrator(stored, version)
		if err != nil {
			return err
		}
		stored = fixed
	}

	newValue := make([]byte, len(stored)+len(appendix))
	copy(newValue, stored)
	copy(newValue[len(stored):], appendix)
	return txn.SetEntry(
		badger.NewEntry(key, newValue).
			WithMeta(config.version).
			WithTTL(config.ttl),
	)
}

// Метод аналогичен appendValue, за исключением того что сохраненные данные воспринимаются
// в качестве Set и в них не могут содержаться одинаковые значения.
//
// На одинаковость значения проверяются через сравнение кусков байт фиксированной длины из дескриптора.
func appendValueIfNotExistsAndFilter(txn *badger.Txn, key, appendix []byte, filter func([]byte) []byte, config *valueDescriptor) error {
	stored, version, err := getWithValue(txn, key)

	if err == badger.ErrKeyNotFound {
		return txn.SetEntry(
			badger.NewEntry(key, appendix).
				WithMeta(config.version).
				WithTTL(config.ttl),
		)
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

		if filter != nil {
			stored = filter(stored)
			if len(stored) == 0 {
				return txn.Delete(key)
			}
		}
	}

	if updated {
		return txn.SetEntry(
			badger.NewEntry(key, stored).
				WithMeta(config.version).
				WithTTL(config.ttl),
		)
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
		return txn.SetEntry(
			badger.NewEntry(key, stored).
				WithMeta(config.version).
				WithTTL(config.ttl),
		)
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
	//l.Printf("DEBUG: "+f, v...)
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

func backup(db *badger.DB, dir string) error {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err := os.Mkdir(dir, os.ModePerm); err != nil {
			return err
		}
	}
	bakName := fmt.Sprint(dir, "/backup", time.Now().Unix(), ".bak")
	file, err := os.Create(bakName)
	if err != nil {
		return err
	}
	defer func() {
		err := file.Close()
		if err != nil {
			log.Println(err)
		}
	}()

	_, err = db.Backup(file, 0)
	if err != nil {
		return err
	}
	return nil
}
