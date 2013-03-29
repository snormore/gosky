package sky

import (
	"testing"
)

const (
	testTableName = "sky-go-integration"
)

// Setup the test environment.
func run(t *testing.T, f func(*Client)) {
	client := NewClient("localhost")
	client.DeleteTable(NewTable(testTableName, nil))
	
	// Create the table.
	err := client.CreateTable(NewTable(testTableName, nil))
	if err != nil {
		t.Fatalf("Unable to setup test table: %v", err)
	}

	f(client)

	client.DeleteTable(NewTable(testTableName, nil))
}


