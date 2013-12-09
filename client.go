package sky

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
)

const (
	DefaultPort = 8585
)

// A Client is what communicates with the server.
type Client interface {
	Host() string
	SetHost(host string)

	Port() uint
	SetPort(port uint)

	// Retrieves a single table from the server.
	GetTable(name string) (Table, error)

	// Retrieves a list of all tables on the server.
	GetTables() ([]Table, error)

	// Creates a table on the server.
	CreateTable(table Table) error

	// Deletes a table on the server.
	DeleteTable(table Table) error

	// Opens a stream to the server and passes the stream to a function for processing.
	// The stream is automatically closed when the function completes.
	Stream(f func(*EventStream)) error

	// Checks if the server is currently running and available.
	Ping() bool

	// Sends and receives raw data sent to a URL path.
	Send(method string, path string, data interface{}, ret interface{}) error

	// Constructs a URL based on the client's host, port and a given path.
	URL(path string) string

	// The HTTP client used by the client.
	HTTPClient() *http.Client
}

type client struct {
	host       string
	port       uint
	httpClient *http.Client
}

func NewClient(host string) Client {
	return &client{
		host:       host,
		port:       DefaultPort,
		httpClient: &http.Client{},
	}
}

func NewClientEx(host string, port uint) Client {
	return &client{
		Host:       host,
		Port:       port,
		httpClient: &http.Client{},
	}
}

// Host retrieves the current host.
func (c *client) Host() string {
	return c.host
}

// SetHost sets the current host.
func (c *client) SetHost(host string) {
	c.host = host
}

// Port retrieves the current port.
func (c *client) Port() uint {
	return c.port
}

// SetPort sets the current port.
func (c *client) SetPort(port uint) {
	c.port = port
}

// The HTTP client.
func (c *client) HTTPClient() *http.Client {
	return c.httpClient
}

// Constructs a URL based on the client's host, port and a given path.
func (c *client) URL(path string) string {
	return fmt.Sprintf("http://%s:%d%s", c.Host(), c.Port(), path)
}

// Sends low-level data to and from the server.
func (c *client) Send(method string, path string, data interface{}, ret interface{}) error {
	url := c.URL(path)

	// Convert the data to JSON.
	var err error
	var body []byte
	if data != nil {
		body, err = json.Marshal(data)
		if err != nil {
			return err
		}
	}

	// Create the request object.
	req, err := http.NewRequest(method, url, strings.NewReader(string(body)))
	if err != nil {
		return err
	}
	req.Header.Add("Content-Type", "application/json")

	// Send the request to the server.
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// If we have a return object then deserialize to it.
	if resp.StatusCode != http.StatusOK {
		h := make(map[string]interface{})
		err := json.NewDecoder(resp.Body).Decode(h)
		if message, ok := h["message"].(string); (err == nil || err == io.EOF) && ok {
			return NewError(message)
		} else {
			return NewError(fmt.Sprintf("sky.Error: \"%s %s\" [%d]", method, url, resp.StatusCode))
		}
	}

	// Deserialize data into return object if we have one.
	if ret != nil {
		err := json.NewDecoder(resp.Body).Decode(ret)
		if err != nil && err != io.EOF {
			return err
		}
	}

	return nil
}

func (c *client) GetTable(name string) (Table, error) {
	if name == "" {
		return nil, errors.New("Table name required")
	}
	table := NewTable("", c)
	if err := c.Send("GET", fmt.Sprintf("/tables/%s", name), nil, table); err != nil {
		return nil, err
	}
	return table, nil
}

func (c *client) GetTables() ([]Table, error) {
	// Retrieve an array of table implementations.
	tables := make([]*table, 0)
	if err := c.Send("GET", "/tables", nil, &tables); err != nil {
		return nil, err
	}

	// Convert to an array of table interfaces.
	tmp := make([]Table, 0)
	for _, t := range tables {
		tmp = append(tmp, t)
	}
	return tmp, nil
}

func (c *client) CreateTable(table Table) error {
	if table == nil {
		return errors.New("Table required")
	}
	table.SetClient(c)
	return c.Send("POST", "/tables", table, table)
}

func (c *client) DeleteTable(table Table) error {
	if table == nil {
		return errors.New("Table required")
	}
	table.SetClient(c)
	return c.Send("DELETE", fmt.Sprintf("/tables/%s", table.Name()), nil, nil)
}

func (c *client) Ping() bool {
	err := c.Send("GET", "/ping", nil, nil)
	return err == nil
}

// Opens a stream to the server and passes the stream to a function for processing.
// The stream is automatically closed when the function completes.
func (c *client) Stream(f func(*EventStream)) error {
	// Send the HTTP request with the reader.
	stream := NewEventStream()
	req, err := http.NewRequest("PATCH", c.URL("/events"), stream.reader)
	if err != nil {
		return err
	}
	req.Header.Add("Content-Type", "application/json")

	// Send the request to the server.
	finished := make(chan interface{})
	go func() {
		resp, err := c.HTTPClient().Do(req)
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
