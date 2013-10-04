package sky

import (
	"errors"
	"fmt"
	"net/http"
	"time"
)

//------------------------------------------------------------------------------
//
// Globals
//
//------------------------------------------------------------------------------

const (
	Replace = "replace"
	Merge   = "merge"
)

//------------------------------------------------------------------------------
//
// Typedefs
//
//------------------------------------------------------------------------------

// A Table is a container for objects and events.
type Table struct {
	client Client
	Name   string `json:"name"`
}

//------------------------------------------------------------------------------
//
// Constructor
//
//------------------------------------------------------------------------------

func NewTable(name string, client Client) *Table {
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
	if err := t.client.Send("GET", fmt.Sprintf("/tables/%s/properties/%s", t.Name, name), nil, property); err != nil {
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
	if err := t.client.Send("GET", fmt.Sprintf("/tables/%s/properties", t.Name), nil, &properties); err != nil {
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
	return t.client.Send("POST", fmt.Sprintf("/tables/%s/properties", t.Name), property, property)
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
	return t.client.Send("PATCH", fmt.Sprintf("/tables/%s/properties/%s", t.Name, name), property, property)
}

// Deletes a property on the table.
func (t *Table) DeleteProperty(property *Property) error {
	if t.client == nil {
		return errors.New("Table is not attached to a client")
	}
	if property == nil {
		return errors.New("Property required")
	}
	return t.client.Send("DELETE", fmt.Sprintf("/tables/%s/properties/%s", t.Name, property.Name), nil, nil)
}

//--------------------------------------
// Event API
//--------------------------------------

// Retrieves a single event for an object.
func (t *Table) GetEvent(objectId string, timestamp time.Time) (*Event, error) {
	if t.client == nil {
		return nil, errors.New("Table is not attached to a client")
	}
	if objectId == "" {
		return nil, errors.New("Object identifier required")
	}

	e := map[string]interface{}{}
	if err := t.client.Send("GET", fmt.Sprintf("/tables/%s/objects/%s/events/%s", t.Name, objectId, FormatTimestamp(timestamp)), nil, &e); err != nil {
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
func (t *Table) GetEvents(objectId string) ([]*Event, error) {
	if t.client == nil {
		return nil, errors.New("Table is not attached to a client")
	}
	if objectId == "" {
		return nil, errors.New("Object identifier required")
	}
	output := make([]map[string]interface{}, 0)
	if err := t.client.Send("GET", fmt.Sprintf("/tables/%s/objects/%s/events", t.Name, objectId), nil, &output); err != nil {
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
func (t *Table) AddEvent(objectId string, event *Event, method string) error {
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
	return t.client.Send(httpMethod, fmt.Sprintf("/tables/%s/objects/%s/events/%s", t.Name, objectId, FormatTimestamp(event.Timestamp)), event.Serialize(), nil)
}

// Deletes an event on the table.
func (t *Table) DeleteEvent(objectId string, event *Event) error {
	if t.client == nil {
		return errors.New("Table is not attached to a client")
	}
	if objectId == "" {
		return errors.New("Object identifier required")
	}
	if event == nil {
		return errors.New("Event required")
	}
	return t.client.Send("DELETE", fmt.Sprintf("/tables/%s/objects/%s/events/%s", t.Name, objectId, FormatTimestamp(event.Timestamp)), nil, nil)
}

// Opens a stream to the server and passes the stream to a function for processing.
// The stream is automatically closed when the function completes.
func (t *Table) Stream(f func(*EventStream)) error {
	// Send the HTTP request with the reader.
	stream := NewEventStream()
	req, err := http.NewRequest("PATCH", t.client.URL(fmt.Sprintf("/tables/%s/events", t.Name)), stream.reader)
	if err != nil {
		return err
	}
	req.Header.Add("Content-Type", "application/json")

	// Send the request to the server.
	finished := make(chan interface{})
	go func() {
		resp, err := t.client.HTTPClient().Do(req)
		if err != nil {
			finished <- err
		} else {
			finished <- resp
		}
	}()

	// Yield to processing function.
	f(stream)

	// Close the stream.
	stream.writer.Close()
	ret := <-finished
	stream.reader.Close()

	// Check if the client errored out and return the error appropriately.
	if resp, ok := ret.(*http.Response); ok {
		defer resp.Body.Close()

		if resp.StatusCode != 200 {
			return fmt.Errorf("Stream error: %d", resp.StatusCode)
		}
		return nil
	} else if err, ok := ret.(error); ok {
		return err
	}
	return nil
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

//--------------------------------------
// Query API
//--------------------------------------

// Retrieves basic stats on the table.
func (t *Table) Stats() (*Stats, error) {
	if t.client == nil {
		return nil, errors.New("Table is not attached to a client")
	}
	output := &Stats{}
	if err := t.client.Send("GET", fmt.Sprintf("/tables/%s/stats", t.Name), nil, &output); err != nil {
		return nil, err
	}
	return output, nil
}

// Executes a raw query on the table.
func (t *Table) RawQuery(q map[string]interface{}) (map[string]interface{}, error) {
	if t.client == nil {
		return nil, errors.New("Table is not attached to a client")
	}
	if q == nil {
		return nil, errors.New("Query required")
	}
	output := map[string]interface{}{}
	if err := t.client.Send("POST", fmt.Sprintf("/tables/%s/query", t.Name), q, &output); err != nil {
		return nil, err
	}
	return output, nil
}

