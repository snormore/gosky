package sky

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
)

//------------------------------------------------------------------------------
//
// Constants
//
//------------------------------------------------------------------------------

const (
	DefaultPort = 8585
)

//------------------------------------------------------------------------------
//
// Typedefs
//
//------------------------------------------------------------------------------

// A Client is what communicates command to the server.
type Client struct {
	Host string
	Port int
	httpClient *http.Client
}

//------------------------------------------------------------------------------
//
// Constructor
//
//------------------------------------------------------------------------------

func NewClient(host string) *Client {
	return &Client{
		Host:    host,
		Port: DefaultPort,
		httpClient: &http.Client{},
	}
}

//------------------------------------------------------------------------------
//
// Methods
//
//------------------------------------------------------------------------------

//--------------------------------------
// HTTP
//--------------------------------------

// Creates a table on the server.
func (c *Client) send(method string, path string, data interface{}, ret interface{}) (error) {
	// Create the URL.
	url := fmt.Sprintf("http://%s:%d%s", c.Host, c.Port, path)

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
		if message, ok := h["message"].(string); err == nil && ok {
			return NewError(message)
		} else {
			return NewError(fmt.Sprintf("sky.Error: \"%s %s\" [%d]", method, url, resp.StatusCode))
		}
	}

	// Deserialize data into return object if we have one.
	if ret != nil {
		err := json.NewDecoder(resp.Body).Decode(ret)
		if err != nil {
			return err
		}
	}
	
	return nil
}

//--------------------------------------
// Table API
//--------------------------------------

// Creates a table on the server.
func (c *Client) CreateTable(table *Table) error {
	if table == nil {
		return errors.New("Table required")
	}
	table.client = c
	return c.send("POST", "/tables", table, table)
}

// Deletes a table on the server.
func (c *Client) DeleteTable(table *Table) error {
	if table == nil {
		return errors.New("Table required")
	}
	table.client = c
	return c.send("DELETE", fmt.Sprintf("/tables/%s", table.Name), table, nil)
}

