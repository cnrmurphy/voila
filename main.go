package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"
)

type Page struct {
	keySize   uint64
	valueSize uint64
	offset    uint64
	size      uint64
}

type KV struct {
	pages      map[string]Page
	f          *os.File
	lastOffset uint64
}

func NewKV() *KV {
	return &KV{pages: make(map[string]Page)}
}

func (kv *KV) Connect() *os.File {
	f, err := os.OpenFile("db.db", os.O_CREATE|os.O_RDWR, os.ModePerm)

	if err != nil {
		log.Println("could not open database file for writing")
		log.Fatal(err)
	}

	log.Println("database connected")
	kv.f = f
	return f
}

func (kv *KV) Insert(key string, value string) error {
	// write only for now to keep things simple - overwriting requires adjusting offsets
	if _, ok := kv.pages[key]; ok == true {
		return fmt.Errorf("cannot overwrite existing key %s", key)
	}

	pageBuffer := make([]byte, 0)

	keySize := uint64(len(key))
	keySizeBuffer := make([]byte, 8)
	keyBuffer := []byte(key)
	binary.LittleEndian.PutUint64(keySizeBuffer, keySize)
	pageBuffer = append(pageBuffer, keySizeBuffer...)
	pageBuffer = append(pageBuffer, keyBuffer...)

	valueSize := uint64(len(value))
	valueSizeBuffer := make([]byte, 8)
	valueBuffer := []byte(value)
	binary.LittleEndian.PutUint64(valueSizeBuffer, valueSize)
	pageBuffer = append(pageBuffer, valueSizeBuffer...)
	pageBuffer = append(pageBuffer, valueBuffer...)

	offset, err := kv.f.WriteAt(pageBuffer, int64(kv.lastOffset))

	if err != nil {
		return err
	}
	page := Page{
		offset:    kv.lastOffset,
		size:      uint64(len(keyBuffer)) + uint64(len(valueBuffer)) + 16,
		valueSize: uint64(len(value)),
		keySize:   uint64(len(key)),
	}
	kv.pages[key] = page
	kv.lastOffset += uint64(offset)

	return nil
}

func (kv *KV) Get(key string) (string, error) {
	if page, ok := kv.pages[key]; ok {
		valueOffset := page.offset + 8 + page.keySize + 8

		// we may be able to store the last read offset to avoid always seeking from the start of the file
		_, err := kv.f.Seek(int64(valueOffset), 0)
		if err != nil {
			log.Panic(err)
		}

		valueBuf := make([]byte, page.valueSize)

		err = binary.Read(kv.f, binary.LittleEndian, valueBuf)

		if err != nil {
			log.Panic(err)
		}

		return string(valueBuf), nil
	} else {
		return "", fmt.Errorf("cannot find key %s", key)
	}
}

func main() {
	kv := NewKV()
	f := kv.Connect()
	defer f.Close()

	err := kv.Insert("hello", "world")
	if err != nil {
		log.Fatal(err)
	}
	err = kv.Insert("foo", "bar")
	if err != nil {
		log.Fatal(err)
	}
	err = kv.Insert("go", "golang")
	if err != nil {
		log.Fatal(err)
	}

	for k, s := range kv.pages {
		v, err := kv.Get(k)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("GET key %s at offset %d -> VALUE %s", k, s.offset, v)
	}
}
