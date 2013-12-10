package sky

import (
	"fmt"
	"os"
	"testing"
)

const (
	testTableName = "sky-go-integration"
)

// Setup the test environment.
func run(t *testing.T, f func(Client, Table)) {
	client := NewClientEx("shopify.dev", 8589)
	if !client.Ping() {
		t.Fatalf("Server is not running")
	}
	client.DeleteTable(NewTable(testTableName, nil))

	// Create the table.
	table := NewTable(testTableName, nil)
	err := client.CreateTable(table)
	if err != nil {
		t.Fatalf("Unable to setup test table: %v", err)
	}

	f(client, table)

	client.DeleteTable(NewTable(testTableName, nil))
}

func warn(msg string, v ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", v...)
}
