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
