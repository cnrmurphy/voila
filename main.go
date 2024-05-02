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

func (kv *KV) loadFromStorage() {
	var keySize uint64
	var valueSize uint64
	var offset int64

	for {
		page := Page{}
		keySizeBuf := make([]byte, 8)
		valueSizeBuf := make([]byte, 8)

		n, err := kv.f.ReadAt(keySizeBuf, offset)
		if err != nil {
			return
		}
		offset += int64(n)
		keySize = binary.LittleEndian.Uint64(keySizeBuf)

		n, err = kv.f.ReadAt(valueSizeBuf, offset)
		if err != nil {
			return
		}
		offset += int64(n)
		valueSize = binary.LittleEndian.Uint64(valueSizeBuf)

		keyBuf := make([]byte, keySize)
		valueBuf := make([]byte, valueSize)
		n, err = kv.f.ReadAt(keyBuf, offset)
		if err != nil {
			return
		}
		offset += int64(n)
		key := string(keyBuf)
		log.Println(key)

		n, err = kv.f.ReadAt(valueBuf, offset)
		if err != nil {
			return
		}
		offset += int64(n)
		value := string(valueBuf)
		log.Println(value)
		page.keySize = keySize
		page.valueSize = valueSize
		page.size = keySize + valueSize + 8 + 8
		page.offset = uint64(offset) - page.size
		kv.pages[key] = page
	}
}

func (kv *KV) Insert(key string, value string) error {
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
		valueOffset := page.offset + 8 + 8 + page.keySize

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

	kv.loadFromStorage()
	for k, s := range kv.pages {
		v, err := kv.Get(k)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("GET key %s at offset %d -> VALUE %s", k, s.offset, v)
	}

	log.Println(kv)

	/*
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
	*/
}
