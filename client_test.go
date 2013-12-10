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

func TestEventStream(t *testing.T) {
	run(t, func(client Client, table Table) {
		err := table.Stream(func(stream *EventStream) {
			var data map[string]interface{}
			now := time.Now()
			for i := 0; i < 10; i++ {
				stream.AddEvent("xyz", NewEvent(now.Add((time.Duration(i)*time.Hour)), data))
			}
		})
		if err != nil {
			t.Fatalf("Failed to create event stream: (%v)", err)
		}
		events, err := table.GetEvents("xyz")
		if err != nil || len(events) != 10 {
			t.Fatalf("Failed to get 10 events back: %d events, (%v)", len(events), err)
		}
	})
}

func TestTableEventStream(t *testing.T) {
	run(t, func(client Client, table Table) {
		err := client.Stream(func(stream *EventStream) {
			var data map[string]interface{}
			now := time.Now()
			for i := 0; i < 10; i++ {
				stream.AddTableEvent("xyz", table, NewEvent(now.Add((time.Duration(i)*time.Hour)), data))
			}
		})
		if err != nil {
			t.Fatalf("Failed to create event stream: (%v)", err)
		}
		events, err := table.GetEvents("xyz")
		if err != nil || len(events) != 10 {
			t.Fatalf("Failed to get 10 events back: %d events, (%v)", len(events), err)
		}
	})
}
