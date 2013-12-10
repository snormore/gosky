package sky

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
)

//------------------------------------------------------------------------------
//
// Typedefs
//
//------------------------------------------------------------------------------

// An event stream maintains an open connection to the database to send events
// in bulk.
type EventStream struct {
	encoder *json.Encoder
	buffer  *bufio.Writer
	conn    net.Conn
}

//------------------------------------------------------------------------------
//
// Constructor
//
//------------------------------------------------------------------------------

func NewEventStream(c Client, table Table) (*EventStream, error) {
	header := fmt.Sprintf("PATCH /tables/%s/events HTTP/1.0\r\nHost: %s\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\n\r\n", table.Name(), c.GetHost())
	return newStream(c, []byte(header))
}

func NewTableEventStream(c Client) (*EventStream, error) {
	header := fmt.Sprintf("PATCH /events HTTP/1.0\r\nHost: %s\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\n\r\n", c.GetHost())
	return newStream(c, []byte(header))
}

func newStream(c Client, header []byte) (*EventStream, error) {
	s := &EventStream{}
	address := fmt.Sprintf("%s:%d", c.GetHost(), c.GetPort())
	fmt.Println("Connecting: ", address)
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return nil, err
	}
	if _, err = conn.Write(header); err != nil {
		conn.Close()
		return nil, err
	}
	s.conn = conn
	s.buffer = bufio.NewWriter(conn)
	s.encoder = json.NewEncoder(s.buffer)
	return s, nil
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
	return s.sendChunk(data)
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
	return s.sendChunk(data)
}

func (s *EventStream) sendChunk(data map[string]interface{}) error {
	fmt.Println("Sending Event : ", data)
	var err error
	var size int
	if data != nil {
		// Encode the serialized data into the stream.
		if err = s.encoder.Encode(data); err != nil {
			return err
		}
		size = s.buffer.Buffered()
	}

	if _, err = fmt.Fprintf(s.conn, "%x\r\n", size); err != nil {
		return err
	}
	if data != nil {
		if err = s.buffer.Flush(); err != nil {
			return err
		}
	} else {
		fmt.Println("Closing chunk!")
	}
	if _, err = fmt.Fprint(s.conn, "\r\n"); err != nil {
		return err
	}
	return nil
}

func (s *EventStream) Close() error {
	defer s.conn.Close()
	if err := s.sendChunk(nil); err != nil {
		fmt.Println("Closing chunk error: ", err)
		return err
	}
	response, err := http.ReadResponse(bufio.NewReader(s.conn), nil)
	if err != nil {
		fmt.Println("Response error: ", err)
		return err
	}
	fmt.Println("Stream response: ", response)
	return nil
}
