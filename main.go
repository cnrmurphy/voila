package main

import (
	"fmt"
	"log"
	"os"
)

type Page struct {
	offset uint64
	size   uint64
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

	offset, err := kv.f.WriteAt([]byte(value), int64(kv.lastOffset))

	if err != nil {
		return err
	}
	page := Page{
		offset: kv.lastOffset,
		size:   uint64(len(value)),
	}
	kv.pages[key] = page
	kv.lastOffset += uint64(offset)

	return nil
}

func (kv *KV) Get(key string) (string, error) {
	if page, ok := kv.pages[key]; ok {
		// we may be able to store the last read offset to avoid always seeking from the start of the file
		_, err := kv.f.Seek(int64(page.offset), 0)
		if err != nil {
			log.Panic(err)
		}

		b := make([]byte, page.size)
		_, err = kv.f.Read(b)

		if err != nil {
			log.Panic(err)
		}

		return string(b), nil
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
