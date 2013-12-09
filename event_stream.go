package sky

import (
	"encoding/json"
	"errors"
	"io"
)

//------------------------------------------------------------------------------
//
// Typedefs
//
//------------------------------------------------------------------------------

// An event stream maintains an open connection to the database to send events
// in bulk.
type EventStream struct {
	reader  *io.PipeReader
	writer  *io.PipeWriter
	encoder *json.Encoder
}

//------------------------------------------------------------------------------
//
// Constructor
//
//------------------------------------------------------------------------------

func NewEventStream() *EventStream {
	s := &EventStream{}
	s.reader, s.writer = io.Pipe()
	s.encoder = json.NewEncoder(s.writer)
	return s
}

//------------------------------------------------------------------------------
//
// Methods
//
//------------------------------------------------------------------------------

//--------------------------------------
// Event API
//--------------------------------------

// Adds an event to an object.
func (s *EventStream) AddEvent(objectId string, event *Event) error {
	if objectId == "" {
		return errors.New("Object identifier required")
	}
	if event == nil {
		return errors.New("Event required")
	}

	// Attach the object identifier at the root of the event.
	data := event.Serialize()
	data["id"] = objectId

	// Encode the serialized data into the stream.
	return s.encoder.Encode(data)
}

func (s *EventStream) AddTableEvent(objectId string, table Table, event *Event) error {
	if objectId == "" {
		return errors.New("Object identifier required")
	}
	if table == nil {
		return errors.New("Table required")
	}
	if event == nil {
		return errors.New("Event required")
	}

	// Attach the object identifier at the root of the event.
	data := event.Serialize()
	data["id"] = objectId
	data["table"] = table.Name()

	// Encode the serialized data into the stream.
	return s.encoder.Encode(data)
}
