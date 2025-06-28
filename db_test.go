package voila

import (
	"path/filepath"
	"testing"
)

func TestNew(t *testing.T) {
	db := New()
	if db == nil {
		t.Fatal("New() returned nil")
	}
	if db.pages == nil {
		t.Fatal("pages map not initialized")
	}
}

func TestOpenAndClose(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")

	db := New()

	err := db.Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	if db.f == nil {
		t.Fatal("File handle not set after Open")
	}

	err = db.Close()
	if err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}
}

func TestInsertAndGet(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")

	db := New()
	err := db.Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Test inserting and getting a value
	key := "test_key"
	value := []byte("test_value")

	err = db.Insert(key, value)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	retrieved, err := db.Get(key)
	if err != nil {
		t.Fatalf("Failed to get: %v", err)
	}

	if string(retrieved) != string(value) {
		t.Fatalf("Expected '%s', got '%s'", string(value), string(retrieved))
	}
}

func TestGetNonExistentKey(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")

	db := New()
	err := db.Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Get("nonexistent")
	if err == nil {
		t.Fatal("Expected error for nonexistent key")
	}
}

func TestMultipleInserts(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")

	db := New()
	err := db.Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Insert multiple key-value pairs
	testData := map[string][]byte{
		"key1": []byte("value1"),
		"key2": []byte("value2"),
		"key3": []byte("value3"),
	}

	for k, v := range testData {
		err = db.Insert(k, v)
		if err != nil {
			t.Fatalf("Failed to insert %s: %v", k, err)
		}
	}

	// Verify all values can be retrieved
	for k, expectedV := range testData {
		retrievedV, err := db.Get(k)
		if err != nil {
			t.Fatalf("Failed to get %s: %v", k, err)
		}
		if string(retrievedV) != string(expectedV) {
			t.Fatalf("For key %s, expected '%s', got '%s'", k, string(expectedV), string(retrievedV))
		}
	}
}

func TestKeys(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")

	db := New()
	err := db.Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	expectedKeys := []string{"key1", "key2", "key3"}

	for _, key := range expectedKeys {
		err = db.Insert(key, []byte("value"))
		if err != nil {
			t.Fatalf("Failed to insert %s: %v", key, err)
		}
	}

	keys := db.Keys()
	if len(keys) != len(expectedKeys) {
		t.Fatalf("Expected %d keys, got %d", len(expectedKeys), len(keys))
	}

	// Create a map for easy checking
	keyMap := make(map[string]bool)
	for _, key := range keys {
		keyMap[key] = true
	}

	for _, expectedKey := range expectedKeys {
		if !keyMap[expectedKey] {
			t.Fatalf("Expected key %s not found in keys list", expectedKey)
		}
	}
}

func TestExists(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")

	db := New()
	err := db.Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	key := "test_key"

	// Key should not exist initially
	if db.Exists(key) {
		t.Fatal("Key should not exist initially")
	}

	// Insert key
	err = db.Insert(key, []byte("value"))
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Key should now exist
	if !db.Exists(key) {
		t.Fatal("Key should exist after insert")
	}
}

func TestPersistence(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")

	// First session: insert data
	{
		db := New()
		err := db.Open(dbPath)
		if err != nil {
			t.Fatalf("Failed to open database: %v", err)
		}

		err = db.Insert("persistent_key", []byte("persistent_value"))
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}

		db.Close()
	}

	// Second session: verify data persists
	{
		db := New()
		err := db.Open(dbPath)
		if err != nil {
			t.Fatalf("Failed to open database: %v", err)
		}
		defer db.Close()

		value, err := db.Get("persistent_key")
		if err != nil {
			t.Fatalf("Failed to get persistent key: %v", err)
		}

		if string(value) != "persistent_value" {
			t.Fatalf("Expected 'persistent_value', got '%s'", string(value))
		}
	}
}

func TestEmptyValue(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")

	db := New()
	err := db.Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	key := "empty_key"
	emptyValue := []byte("")

	err = db.Insert(key, emptyValue)
	if err != nil {
		t.Fatalf("Failed to insert empty value: %v", err)
	}

	retrieved, err := db.Get(key)
	if err != nil {
		t.Fatalf("Failed to get empty value: %v", err)
	}

	if len(retrieved) != 0 {
		t.Fatalf("Expected empty value, got '%s'", string(retrieved))
	}
}

func TestBinaryValue(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")

	db := New()
	err := db.Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	key := "binary_key"
	binaryValue := []byte{0x00, 0x01, 0xFF, 0xFE, 0x42}

	err = db.Insert(key, binaryValue)
	if err != nil {
		t.Fatalf("Failed to insert binary value: %v", err)
	}

	retrieved, err := db.Get(key)
	if err != nil {
		t.Fatalf("Failed to get binary value: %v", err)
	}

	if len(retrieved) != len(binaryValue) {
		t.Fatalf("Binary value length mismatch: expected %d, got %d", len(binaryValue), len(retrieved))
	}

	for i, b := range binaryValue {
		if retrieved[i] != b {
			t.Fatalf("Binary value mismatch at index %d: expected %x, got %x", i, b, retrieved[i])
		}
	}
}

func TestOperationsOnClosedDB(t *testing.T) {
	db := New()

	// Test operations on unopened database
	err := db.Insert("key", []byte("value"))
	if err == nil {
		t.Fatal("Expected error when inserting to unopened database")
	}

	_, err = db.Get("key")
	if err == nil {
		t.Fatal("Expected error when getting from unopened database")
	}
}
