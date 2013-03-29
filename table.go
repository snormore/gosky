package sky

import (
	"errors"
	"fmt"
)

//------------------------------------------------------------------------------
//
// Typedefs
//
//------------------------------------------------------------------------------

// A Table is a container for objects and events.
type Table struct {
	client *Client
	Name   string `json:"name"`
}

//------------------------------------------------------------------------------
//
// Constructor
//
//------------------------------------------------------------------------------

func NewTable(name string, client *Client) *Table {
	return &Table{
		Name:   name,
		client: client,
	}
}

//------------------------------------------------------------------------------
//
// Methods
//
//------------------------------------------------------------------------------

//--------------------------------------
// Property API
//--------------------------------------

// Retrieves a single property from the server.
func (t *Table) GetProperty(name string) (*Property, error) {
	if t.client == nil {
		return nil, errors.New("Table is not attached to a client")
	}
	if name == "" {
		return nil, errors.New("Property name required")
	}
	property := &Property{}
	if err := t.client.send("GET", fmt.Sprintf("/tables/%s/properties/%s", t.Name, name), nil, property); err != nil {
		return nil, err
	}
	return property, nil
}

// Retrieves a list of all properties on the table.
func (t *Table) GetProperties() ([]*Property, error) {
	if t.client == nil {
		return nil, errors.New("Table is not attached to a client")
	}
	properties := []*Property{}
	if err := t.client.send("GET", fmt.Sprintf("/tables/%s/properties", t.Name), nil, &properties); err != nil {
		return nil, err
	}
	return properties, nil
}

// Creates a property on the table.
func (t *Table) CreateProperty(property *Property) error {
	if t.client == nil {
		return errors.New("Table is not attached to a client")
	}
	if property == nil {
		return errors.New("Property required")
	}
	return t.client.send("POST", fmt.Sprintf("/tables/%s/properties", t.Name), property, property)
}

// Updates a property on the table.
func (t *Table) UpdateProperty(name string, property *Property) error {
	if t.client == nil {
		return errors.New("Table is not attached to a client")
	}
	if name == "" {
		return errors.New("Property name required")
	}
	if property == nil {
		return errors.New("Property required")
	}
	return t.client.send("PATCH", fmt.Sprintf("/tables/%s/properties/%s", t.Name, name), property, property)
}

// Deletes a property on the table.
func (t *Table) DeleteProperty(property *Property) error {
	if t.client == nil {
		return errors.New("Table is not attached to a client")
	}
	if property == nil {
		return errors.New("Property required")
	}
	return t.client.send("DELETE", fmt.Sprintf("/tables/%s/properties/%s", t.Name, property.Name), nil, nil)
}
