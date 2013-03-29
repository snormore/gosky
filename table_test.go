package sky

import (
	"testing"
)

//--------------------------------------
// Property API
//--------------------------------------

// Ensure that we can create and delete properties.
func TestCreateDeleteProperty(t *testing.T) {
	run(t, func(client *Client, table *Table) {
		property := NewProperty("purchase_price", true, String)

		// Create the property.
		err := table.CreateProperty(property)
		if err != nil {
			t.Fatalf("Unable to create property: %v (%v)", property, err)
		}

		// Delete the property.
		err = table.DeleteProperty(property)
		if err != nil {
			t.Fatalf("Unable to delete property: %v (%v)", property, err)
		}
	})
}

// Ensure that we can get a single property.
func TestGetProperties(t *testing.T) {
	run(t, func(client *Client, table *Table) {
		table.CreateProperty(NewProperty("gender", false, Factor))
		table.CreateProperty(NewProperty("name", false, String))
		table.CreateProperty(NewProperty("myNum", true, Integer))

		// Get a single property.
		p, err := table.GetProperty("gender")
		if err != nil || p == nil || p.Id != 1 || p.Name != "gender" || p.Transient || p.DataType != Factor {
			t.Fatalf("Unable to get property: %v (%v)", p, err)
		}

		// Update property.
		table.UpdateProperty("gender", NewProperty("gender2", true, Integer))

		// Get all properties.
		properties, err := table.GetProperties()
		if err != nil || len(properties) != 3 {
			t.Fatalf("Unable to get properties: %d (%v)", len(properties), err)
		}
		p = properties[0]
		if p.Id != -1 || p.Name != "myNum" || !p.Transient || p.DataType != Integer {
			t.Fatalf("Unable to get properties(0): %v (%v)", p, err)
		}
		p = properties[1]
		if p.Id != 1 || p.Name != "gender2" || p.Transient || p.DataType != Factor {
			t.Fatalf("Unable to get properties(1): %v (%v)", p, err)
		}
		p = properties[2]
		if p.Id != 2 || p.Name != "name" || p.Transient || p.DataType != String {
			t.Fatalf("Unable to get properties(2): %v (%v)", p, err)
		}
	})
}

//--------------------------------------
// Event API
//--------------------------------------

// Ensure that we can replace an event into another one.
func TestReplaceEvent(t *testing.T) {
	run(t, func(client *Client, table *Table) {
		timestamp, _ := ParseTimestamp("1970-01-01T00:00:01.5Z")
		table.CreateProperty(NewProperty("p0", false, Factor))
		table.CreateProperty(NewProperty("t0", true, Integer))
		e0 := NewEvent(timestamp, map[string]interface{}{"p0": "foo", "t0": 10})
		e1 := NewEvent(timestamp, map[string]interface{}{"t0": 20})

		// Add the event.
		err := table.AddEvent("o0", e0, Replace)
		if err != nil {
			t.Fatalf("Unable to replace event: %v (%v)", e0, err)
		}

		// Replace the event.
		err = table.AddEvent("o0", e1, Replace)
		if err != nil {
			t.Fatalf("Unable to replace event: %v (%v)", e1, err)
		}

		// Get the event to verify.
		event, err := table.GetEvent("o0", timestamp)
		if err != nil || event.Data["t0"] != float64(20) || event.Data["p0"] != nil {
			t.Fatalf("Incorrect replaced event: %v (%v)", event, err)
		}
	})
}

// Ensure that we can merge an event into another one.
func TestMergeEvent(t *testing.T) {
	run(t, func(client *Client, table *Table) {
		timestamp, _ := ParseTimestamp("1970-01-01T00:00:01.5Z")
		table.CreateProperty(NewProperty("p0", false, String))
		table.CreateProperty(NewProperty("t0", true, Integer))
		e0 := NewEvent(timestamp, map[string]interface{}{"p0": "foo", "t0": 10})
		e1 := NewEvent(timestamp, map[string]interface{}{"t0": 20})

		// Add the event.
		err := table.AddEvent("o0", e0, Merge)
		if err != nil {
			t.Fatalf("Unable to merge event: %v (%v)", e0, err)
		}

		// Merge the event.
		err = table.AddEvent("o0", e1, Merge)
		if err != nil {
			t.Fatalf("Unable to replace event: %v (%v)", e1, err)
		}

		// Get the event to verify.
		event, err := table.GetEvent("o0", timestamp)
		if err != nil || event.Data["t0"] != float64(20) || event.Data["p0"] != "foo" {
			t.Fatalf("Incorrect merged event: %v (%v)", event, err)
		}
	})
}

// Ensure that we can delete an event.
func TestDeleteEvent(t *testing.T) {
	run(t, func(client *Client, table *Table) {
		timestamp, _ := ParseTimestamp("1970-01-01T00:00:01.5Z")
		table.CreateProperty(NewProperty("p0", false, String))
		table.CreateProperty(NewProperty("t0", true, Integer))
		e0 := NewEvent(timestamp, map[string]interface{}{"p0": "foo", "t0": 10})

		// Add the event.
		err := table.AddEvent("o0", e0, Merge)
		if err != nil {
			t.Fatalf("Unable to merge event: %v (%v)", e0, err)
		}

		// Delete the event.
		err = table.DeleteEvent("o0", e0)
		if err != nil {
			t.Fatalf("Unable to delete event: %v (%v)", e0, err)
		}

		// Get the event to verify.
		event, err := table.GetEvent("o0", timestamp)
		if err != nil || event.Data["t0"] != nil || event.Data["p0"] != nil {
			t.Fatalf("Incorrect deleted event: %v (%v)", event, err)
		}
	})
}
