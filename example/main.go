package main

import (
	"fmt"
	"log"

	"github.com/cnrmurphy/voila"
)

func main() {
	// Create a new database instance
	db := voila.New()

	// Open the database file
	err := db.Open("example.db")
	if err != nil {
		log.Fatal("Failed to open database:", err)
	}
	defer db.Close()

	// Insert some data
	fmt.Println("Inserting data...")
	data := map[string][]byte{
		"name":    []byte("Voila Database"),
		"version": []byte("1.0.0"),
		"author":  []byte("Your Name"),
		"binary":  {0x48, 0x65, 0x6c, 0x6c, 0x6f}, // "Hello" in bytes
	}

	for key, value := range data {
		err := db.Insert(key, value)
		if err != nil {
			log.Fatalf("Failed to insert %s: %v", key, err)
		}
		fmt.Printf("✓ Inserted %s\n", key)
	}

	// Retrieve and display data
	fmt.Println("\nRetrieving data...")
	for _, key := range db.Keys() {
		value, err := db.Get(key)
		if err != nil {
			log.Fatalf("Failed to get %s: %v", key, err)
		}

		if key == "binary" {
			fmt.Printf("Key: %s -> Value: %v (binary)\n", key, value)
		} else {
			fmt.Printf("Key: %s -> Value: %s\n", key, string(value))
		}
	}

	// Check if keys exist
	fmt.Println("\nChecking key existence...")
	testKeys := []string{"name", "nonexistent", "version"}
	for _, key := range testKeys {
		exists := db.Exists(key)
		fmt.Printf("Key '%s' exists: %t\n", key, exists)
	}

	fmt.Println("\nDatabase operations completed successfully!")
	fmt.Println("Data will persist between runs - try running this example again!")
}
