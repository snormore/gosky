package sky

import (
	"testing"
	"time"
)

//--------------------------------------
// Table API
//--------------------------------------

// Ensure that we can retrieve a single table.
func TestGetTable(t *testing.T) {
	run(t, func(client Client, _ Table) {
		table, err := client.GetTable("sky-go-integration")
		if err != nil || table == nil || table.Name() != "sky-go-integration" {
			t.Fatalf("Unable to get table: %v (%v)", table, err)
		}
	})
}

// Ensure that we retrieve a list of all tables.
func TestGetTables(t *testing.T) {
	run(t, func(client Client, _ Table) {
		tables, err := client.GetTables()
		if err != nil || len(tables) == 0 {
			t.Fatalf("Unable to get tables: %d (%v)", tables, err)
		}
	})
}

func TestTableEventStream(t *testing.T) {
	run(t, func(client Client, table Table) {
		stream, err := client.Stream()
		if err != nil {
			t.Fatalf("Failed to create event stream: (%v)", err)
		}
		defer stream.Close()
		var data map[string]interface{}
		now := time.Now()
		for i := 0; i < 10; i++ {
			timestamp := now.Add(time.Duration(i) * time.Hour)
			event := NewEvent(timestamp, data)
			err = stream.AddTableEvent("xyz", table, event)
			if err != nil {
				t.Fatalf("Failed to create event #%d: %v (%v)", i, event, err)
			}
		}
		events, err := table.GetEvents("xyz")
		if err != nil || len(events) != 10 {
			t.Fatalf("Failed to get 10 events back: %d events, (%v)", len(events), err)
		}
	})
}

func TestEventStream(t *testing.T) {
	run(t, func(client Client, table Table) {
		stream, err := table.Stream()
		if err != nil {
			t.Fatalf("Failed to create event stream: (%v)", err)
		}
		defer stream.Close()
		var data map[string]interface{}
		now := time.Now()
		for i := 0; i < 10; i++ {
			timestamp := now.Add(time.Duration(i) * time.Hour)
			event := NewEvent(timestamp, data)
			err = stream.AddEvent("xyz", event)
			if err != nil {
				t.Fatalf("Failed to create event #%d: %v (%v)", i, event, err)
			}
		}
		events, err := table.GetEvents("xyz")
		if err != nil || len(events) != 10 {
			t.Fatalf("Failed to get 10 events back: %d events, (%v)", len(events), err)
		}
	})
}
