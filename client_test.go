package sky

import (
	"testing"
)

// Ensure that we create and delete a table.
func TestCreateDeleteTable(t *testing.T) {
	client := setup(t)
	table := NewTable(testTableName)

	// Create the table.
	err := client.CreateTable(table)
	if err != nil {
		t.Fatalf("Unable to create table: %v", err)
	}

	// Delete the table.
	err = client.DeleteTable(table)
	if err != nil {
		t.Fatalf("Unable to delete table: %v", err)
	}
}


