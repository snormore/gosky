package sky

import (
	"testing"
)

//--------------------------------------
// Table API
//--------------------------------------

// Ensure that we can retrieve a single table.
func TestGetTable(t *testing.T) {
	run(t, func(client Client, _ *Table) {
		table, err := client.GetTable("sky-go-integration")
		if err != nil || table == nil || table.Name != "sky-go-integration" {
			t.Fatalf("Unable to get table: %v (%v)", table, err)
		}
	})
}

// Ensure that we retrieve a list of all tables.
func TestGetTables(t *testing.T) {
	run(t, func(client Client, _ *Table) {
		tables, err := client.GetTables()
		if err != nil || len(tables) == 0 {
			t.Fatalf("Unable to get tables: %d (%v)", tables, err)
		}
	})
}

