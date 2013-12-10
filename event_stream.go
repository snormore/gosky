package sky

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
)

//------------------------------------------------------------------------------
//
// Typedefs
//
//------------------------------------------------------------------------------

// An event stream maintains an open connection to the database to send events
// in bulk. This is the base stream type.
type Stream struct {
	client  Client
	header  []byte
	encoder *json.Encoder
	chunker *chunkWriter
	buffer  *bufio.Writer
	conn    net.Conn
}

// EventStream is a table-less stream.
type EventStream struct {
	*Stream
}

// TableEventStream is a stream to a specific table.
type TableEventStream struct {
	*Stream
	table Table
}

//------------------------------------------------------------------------------
//
// Constructor
//
//------------------------------------------------------------------------------

func NewTableEventStream(c Client, table Table) (*TableEventStream, error) {
	header := fmt.Sprintf("PATCH /tables/%s/events HTTP/1.0\r\nHost: %s\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\n\r\n", table.Name(), c.GetHost())
	s := &TableEventStream{&Stream{client: c, header: []byte(header)}, table}
	return s, s.Reconnect()
}

func NewEventStream(c Client) (*EventStream, error) {
	header := fmt.Sprintf("PATCH /events HTTP/1.0\r\nHost: %s\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\n\r\n", c.GetHost())
	s := &EventStream{&Stream{client: c, header: []byte(header)}}
	return s, s.Reconnect()
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
func (s *TableEventStream) AddEvent(objectId string, event *Event) error {
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

// Adds an event to an object.
func (s *EventStream) AddEvent(objectId string, table Table, event *Event) error {
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
	return s.encoder.Encode(data)
}

func (s *Stream) Flush() error {
	return s.buffer.Flush()
}

func (s *Stream) Close() error {
	defer s.conn.Close()
	if err := s.Flush(); err != nil {
		return err
	}
	if _, err := s.chunker.Write([]byte{}); err != nil {
		return err
	}
	response, err := http.ReadResponse(bufio.NewReader(s.conn), nil)
	if err != nil {
		return err
	}
	response.Body.Close()
	if response.StatusCode != 200 {
		return errors.New(response.Status)
	}
	return nil
}

func (s *Stream) Reconnect() error {
	if s.conn != nil {
		s.conn.Close()
	}
	address := fmt.Sprintf("%s:%d", s.client.GetHost(), s.client.GetPort())
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return err
	}
	if _, err = conn.Write(s.header); err != nil {
		conn.Close()
		return err
	}
	s.conn = conn
	s.chunker = &chunkWriter{conn}
	s.buffer = bufio.NewWriter(s.chunker)
	s.encoder = json.NewEncoder(s.buffer)
	return nil
}

type chunkWriter struct {
	w io.Writer
}

func (cw *chunkWriter) Write(p []byte) (int, error) {
	var err error
	if _, err = fmt.Fprintf(cw.w, "%x\r\n", len(p)); err != nil {
		return 0, err
	}
	var total, count int
	for len(p) > 0 {
		count, err = cw.w.Write(p)
		if !(count > 0) {
			break
		}
		p = p[count:]
		total += count
	}
	if err != nil {
		return total, err
	}
	if _, err = fmt.Fprint(cw.w, "\r\n"); err != nil {
		return total, err
	}
	return total, nil
}
