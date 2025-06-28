package voila

import (
	"encoding/binary"
	"fmt"
	"os"
)

// Page represents the layout of data on disk.
// A Page on disk contains the following in order: key size, value size, value and data.
//
//	+----------+------------+-----------+-----------+
//	|        Header         |         Data          |
//	+----------+------------+-----------+-----------+
//	| 8 bytes  | 8 bytes    | *64 bytes | *64 bytes |
//	+----------+------------+-----------+-----------+
//	| Key Size | Value Size | Key       | Value     |
//	+----------+------------+-----------+-----------+
//
// Because we only store keys in memory, the value is omitted from the Page struct.
type Page struct {
	keySize   uint64
	valueSize uint64
	offset    uint64
	size      uint64
}

// DB represents the key-value database instance
type DB struct {
	pages      map[string]Page
	f          *os.File
	lastOffset uint64
}

// New creates a new database instance
func New() *DB {
	return &DB{pages: make(map[string]Page)}
}

// Open opens or creates a database file and loads existing data
func (db *DB) Open(filename string) error {
	f, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, os.ModePerm)
	if err != nil {
		return fmt.Errorf("could not open database file: %w", err)
	}

	db.f = f
	db.loadFromStorage()
	return nil
}

// Close closes the database file
func (db *DB) Close() error {
	if db.f != nil {
		return db.f.Close()
	}
	return nil
}

func (db *DB) loadFromStorage() {
	var keySize uint64
	var valueSize uint64
	var offset int64

	for {
		page := Page{}
		keySizeBuf := make([]byte, 8)
		valueSizeBuf := make([]byte, 8)

		n, err := db.f.ReadAt(keySizeBuf, offset)
		if err != nil {
			return
		}
		offset += int64(n)
		keySize = binary.LittleEndian.Uint64(keySizeBuf)

		n, err = db.f.ReadAt(valueSizeBuf, offset)
		if err != nil {
			return
		}
		offset += int64(n)
		valueSize = binary.LittleEndian.Uint64(valueSizeBuf)

		keyBuf := make([]byte, keySize)
		valueBuf := make([]byte, valueSize)
		n, err = db.f.ReadAt(keyBuf, offset)
		if err != nil {
			return
		}
		offset += int64(n)
		key := string(keyBuf)

		n, err = db.f.ReadAt(valueBuf, offset)
		if err != nil {
			return
		}
		offset += int64(n)

		page.keySize = keySize
		page.valueSize = valueSize
		page.size = keySize + valueSize + 8 + 8
		page.offset = uint64(offset) - page.size
		db.pages[key] = page
		db.lastOffset = uint64(offset)
	}
}

// Insert adds a new key-value pair to the database
func (db *DB) Insert(key string, value []byte) error {
	if db.f == nil {
		return fmt.Errorf("database not opened")
	}

	pageBuffer := make([]byte, 0)

	keySize := uint64(len(key))
	keySizeBuffer := make([]byte, 8)
	keyBuffer := []byte(key)
	binary.LittleEndian.PutUint64(keySizeBuffer, keySize)
	pageBuffer = append(pageBuffer, keySizeBuffer...)

	valueSize := uint64(len(value))
	valueSizeBuffer := make([]byte, 8)
	valueBuffer := []byte(value)
	binary.LittleEndian.PutUint64(valueSizeBuffer, valueSize)
	pageBuffer = append(pageBuffer, valueSizeBuffer...)

	pageBuffer = append(pageBuffer, keyBuffer...)
	pageBuffer = append(pageBuffer, valueBuffer...)

	_, err := db.f.WriteAt(pageBuffer, int64(db.lastOffset))
	if err != nil {
		return fmt.Errorf("failed to write to database: %w", err)
	}

	page := Page{
		offset:    db.lastOffset,
		size:      uint64(len(keyBuffer)) + uint64(len(valueBuffer)) + 16,
		valueSize: uint64(len(value)),
		keySize:   uint64(len(key)),
	}
	db.pages[key] = page
	db.lastOffset += uint64(len(pageBuffer))

	return nil
}

// Get retrieves a value by key from the database
func (db *DB) Get(key string) ([]byte, error) {
	if db.f == nil {
		return nil, fmt.Errorf("database not opened")
	}

	page, ok := db.pages[key]
	if !ok {
		return nil, fmt.Errorf("key not found: %s", key)
	}

	valueOffset := page.offset + 8 + 8 + page.keySize

	_, err := db.f.Seek(int64(valueOffset), 0)
	if err != nil {
		return nil, fmt.Errorf("failed to seek to value: %w", err)
	}

	valueBuf := make([]byte, page.valueSize)
	err = binary.Read(db.f, binary.LittleEndian, valueBuf)
	if err != nil {
		return nil, fmt.Errorf("failed to read value: %w", err)
	}

	return valueBuf, nil
}

// Keys returns all keys in the database
func (db *DB) Keys() []string {
	keys := make([]string, 0, len(db.pages))
	for k := range db.pages {
		keys = append(keys, k)
	}
	return keys
}

// Exists checks if a key exists in the database
func (db *DB) Exists(key string) bool {
	_, exists := db.pages[key]
	return exists
}
