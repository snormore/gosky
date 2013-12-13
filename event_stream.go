package sky

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
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
func (s *EventStream) AddEvent(table Table, objectId string, event *Event) error {
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

// Send any buffered events to the server
func (s *Stream) Flush() error {
	return s.buffer.Flush()
}

// Close the event stream
func (s *Stream) Close() error {
	defer s.conn.Close()

	// Flush any buffered events
	if err := s.Flush(); err != nil {
		return err
	}

	// Write an empty chunk
	if _, err := s.chunker.Write([]byte{}); err != nil {
		return err
	}

	// Check server response status
	reader := bufio.NewReader(s.conn)
	status, err := reader.ReadString('\r')
	if err != nil {
		return err
	}
	if strings.HasPrefix(status, "HTTP/1.0 200") {
		return nil
	}
	return errors.New(status)
}

// Attempt to reconnect the event stream with the server
func (s *Stream) Reconnect() error {

	// Close the existing connection
	if s.conn != nil {
		s.conn.Close()
		s.conn = nil
	}

	// Open new connection
	address := fmt.Sprintf("%s:%d", s.client.GetHost(), s.client.GetPort())
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return err
	}

	// Write the request header (chunked transfer encoding)
	if _, err = conn.Write(s.header); err != nil {
		conn.Close()
		return err
	}

	// Finish setting up the stream
	s.conn = conn
	s.chunker = &chunkWriter{conn}
	s.buffer = bufio.NewWriter(s.chunker)
	s.encoder = json.NewEncoder(s.buffer)
	return nil
}

// chunkWriter is an io.Writer that will emit any writes in HTTP chunk format
type chunkWriter struct {
	w io.Writer
}

func (cw *chunkWriter) Write(p []byte) (int, error) {
	var err error

	// Emit the chunk header
	if _, err = fmt.Fprintf(cw.w, "%x\r\n", len(p)); err != nil {
		return 0, err
	}

	// Emit the chunk body
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

	// Emit chunk trailer
	if _, err = fmt.Fprint(cw.w, "\r\n"); err != nil {
		return total, err
	}
	return total, nil
}
