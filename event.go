package sky

import (
	"errors"
	"fmt"
	"time"
)

//------------------------------------------------------------------------------
//
// Typedefs
//
//------------------------------------------------------------------------------

// An Event is a timestamped hash of data.
type Event struct {
	Timestamp time.Time
	Data      map[string]interface{}
}

//------------------------------------------------------------------------------
//
// Constructor
//
//------------------------------------------------------------------------------

func NewEvent(timestamp time.Time, data map[string]interface{}) *Event {
	return &Event{
		Timestamp: timestamp,
		Data:      data,
	}
}

//------------------------------------------------------------------------------
//
// Methods
//
//------------------------------------------------------------------------------

//--------------------------------------
// Serialization
//--------------------------------------

// Encodes an event into an untyped map.
func (e *Event) Serialize() map[string]interface{} {
	return map[string]interface{}{
		"timestamp": FormatTimestamp(e.Timestamp),
		"data":      e.Data,
	}
}

// Decodes an event from an untyped map.
func (e *Event) Deserialize(obj map[string]interface{}) error {
	if obj == nil {
		return errors.New("sky.Event: Unable to deserialize nil.")
	}

	// Deserialize "timestamp".
	if str, ok := obj["timestamp"].(string); ok {
		if timestamp, err := ParseTimestamp(str); err == nil {
			e.Timestamp = timestamp
		} else {
			return err
		}
	} else {
		return fmt.Errorf("sky.Event: Invalid timestamp: %v", obj["timestamp"])
	}

	// Deserialize "data".
	if data, ok := obj["data"].(map[string]interface{}); ok {
		e.Data = data
	} else if data == nil {
		e.Data = map[string]interface{}{}
	} else {
		return fmt.Errorf("sky.Event: Invalid data: %v", obj["data"])
	}

	return nil
}
