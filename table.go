package sky

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

const (
	Replace = "replace"
	Merge   = "merge"
)

// A Table is a container for objects and events.
type Table interface {
	// Retrieves the name of the table.
	Name() string

	// Retrieves the client associated with the table.
	Client() Client

	// Sets the client associated with the table.
	SetClient(client Client)

	// Retrieves a single property from the server.
	GetProperty(name string) (*Property, error)

	// Retrieves a list of all properties on the table.
	GetProperties() ([]*Property, error)

	// Creates a property on the table.
	CreateProperty(property *Property) error

	// Updates a property on the table.
	UpdateProperty(name string, property *Property) error

	// Deletes a property on the table.
	DeleteProperty(property *Property) error

	// Retrieves a single event for an object.
	GetEvent(objectId string, timestamp time.Time) (*Event, error)

	// Retrieves a list of all events for an object.
	GetEvents(objectId string) ([]*Event, error)

	// Adds an event to an object.
	AddEvent(objectId string, event *Event, method string) error

	// Deletes an event on the table.
	DeleteEvent(objectId string, event *Event) error

	// Opens a table specific event stream to the server.
	Stream() (*TableEventStream, error)

	// Retrieves basic stats on the table.
	Stats() (*Stats, error)

	// Executes a raw query on the table.
	RawQuery(q map[string]interface{}) (map[string]interface{}, error)
}

type table struct {
	client Client
	name   string `json:"name"`
}

// Creates a new table attached to a given client.
func NewTable(name string, c Client) Table {
	return &table{
		name:   name,
		client: c,
	}
}

// Retrieves a single property from the server.
func (t *table) Name() string {
	return t.name
}

// Retrieves the client associated with the table.
func (t *table) Client() Client {
	return t.client
}

// Retrieves the client associated with the table.
func (t *table) SetClient(c Client) {
	t.client = c
}

// Retrieves a single property from the server.
func (t *table) GetProperty(name string) (*Property, error) {
	if t.client == nil {
		return nil, errors.New("Table is not attached to a client")
	}
	if name == "" {
		return nil, errors.New("Property name required")
	}
	property := &Property{}
	if err := t.client.Send("GET", fmt.Sprintf("/tables/%s/properties/%s", t.name, name), nil, property); err != nil {
		return nil, err
	}
	return property, nil
}

// Retrieves a list of all properties on the table.
func (t *table) GetProperties() ([]*Property, error) {
	if t.client == nil {
		return nil, errors.New("Table is not attached to a client")
	}
	properties := []*Property{}
	if err := t.client.Send("GET", fmt.Sprintf("/tables/%s/properties", t.name), nil, &properties); err != nil {
		return nil, err
	}
	return properties, nil
}

// Creates a property on the table.
func (t *table) CreateProperty(property *Property) error {
	if t.client == nil {
		return errors.New("Table is not attached to a client")
	}
	if property == nil {
		return errors.New("Property required")
	}
	return t.client.Send("POST", fmt.Sprintf("/tables/%s/properties", t.name), property, property)
}

// Updates a property on the table.
func (t *table) UpdateProperty(name string, property *Property) error {
	if t.client == nil {
		return errors.New("Table is not attached to a client")
	}
	if name == "" {
		return errors.New("Property name required")
	}
	if property == nil {
		return errors.New("Property required")
	}
	return t.client.Send("PATCH", fmt.Sprintf("/tables/%s/properties/%s", t.name, name), property, property)
}

// Deletes a property on the table.
func (t *table) DeleteProperty(property *Property) error {
	if t.client == nil {
		return errors.New("Table is not attached to a client")
	}
	if property == nil {
		return errors.New("Property required")
	}
	return t.client.Send("DELETE", fmt.Sprintf("/tables/%s/properties/%s", t.name, property.Name), nil, nil)
}

// Retrieves a single event for an object.
func (t *table) GetEvent(objectId string, timestamp time.Time) (*Event, error) {
	if t.client == nil {
		return nil, errors.New("Table is not attached to a client")
	}
	if objectId == "" {
		return nil, errors.New("Object identifier required")
	}

	e := map[string]interface{}{}
	if err := t.client.Send("GET", fmt.Sprintf("/tables/%s/objects/%s/events/%s", t.name, objectId, FormatTimestamp(timestamp)), nil, &e); err != nil {
		return nil, err
	}

	// Deserialize event data.
	event := &Event{}
	if err := event.Deserialize(e); err != nil {
		return nil, err
	}
	return event, nil
}

// Retrieves a list of all events for an object.
func (t *table) GetEvents(objectId string) ([]*Event, error) {
	if t.client == nil {
		return nil, errors.New("Table is not attached to a client")
	}
	if objectId == "" {
		return nil, errors.New("Object identifier required")
	}
	output := make([]map[string]interface{}, 0)
	if err := t.client.Send("GET", fmt.Sprintf("/tables/%s/objects/%s/events", t.name, objectId), nil, &output); err != nil {
		return nil, err
	}

	// Deserialize.
	events := []*Event{}
	for _, i := range output {
		event := &Event{}
		event.Deserialize(i)
		events = append(events, event)
	}
	return events, nil
}

// Adds an event to an object.
func (t *table) AddEvent(objectId string, event *Event, method string) error {
	if objectId == "" {
		return errors.New("Object identifier required")
	}
	if event == nil {
		return errors.New("Event required")
	}

	// Determine correct HTTP method.
	httpMethod, err := getInsertHttpMethod(method)
	if err != nil {
		return err
	}

	// Serialize data and send to server.
	return t.client.Send(httpMethod, fmt.Sprintf("/tables/%s/objects/%s/events/%s", t.name, objectId, FormatTimestamp(event.Timestamp)), event.Serialize(), nil)
}

// Deletes an event on the table.
func (t *table) DeleteEvent(objectId string, event *Event) error {
	if t.client == nil {
		return errors.New("Table is not attached to a client")
	}
	if objectId == "" {
		return errors.New("Object identifier required")
	}
	if event == nil {
		return errors.New("Event required")
	}
	return t.client.Send("DELETE", fmt.Sprintf("/tables/%s/objects/%s/events/%s", t.name, objectId, FormatTimestamp(event.Timestamp)), nil, nil)
}

func (t *table) Stream() (*TableEventStream, error) {
	return NewTableEventStream(t.client, t)
}

// Determines the appropriate HTTP method to use given an insertion method (Replace, Merge).
func getInsertHttpMethod(method string) (string, error) {
	switch method {
	case Replace:
		return "PUT", nil
	case Merge:
		return "PATCH", nil
	}
	return "", fmt.Errorf("Invalid add event method: %s", method)
}

// Retrieves basic stats on the table.
func (t *table) Stats() (*Stats, error) {
	if t.client == nil {
		return nil, errors.New("Table is not attached to a client")
	}
	output := &Stats{}
	if err := t.client.Send("GET", fmt.Sprintf("/tables/%s/stats", t.name), nil, &output); err != nil {
		return nil, err
	}
	return output, nil
}

// Executes a raw query on the table.
func (t *table) RawQuery(q map[string]interface{}) (map[string]interface{}, error) {
	if t.client == nil {
		return nil, errors.New("Table is not attached to a client")
	}
	if q == nil {
		return nil, errors.New("Query required")
	}
	output := map[string]interface{}{}
	if err := t.client.Send("POST", fmt.Sprintf("/tables/%s/query", t.name), q, &output); err != nil {
		return nil, err
	}
	return output, nil
}

func (t *table) MarshalJSON() ([]byte, error) {
	b, err := json.Marshal(map[string]interface{}{"name": t.name})
	return b, err
}

func (t *table) UnmarshalJSON(data []byte) error {
	tmp := map[string]interface{}{}
	if err := json.Unmarshal(data, &tmp); err != nil {
		return err
	}
	t.name, _ = tmp["name"].(string)
	return nil
}
