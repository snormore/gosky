package sky

import (
	"testing"
)

const (
	testTableName = "sky-go-integration"
)

// Setup the test environment.
func setup(t *testing.T) *Client {
	client := NewClient("localhost")
	client.DeleteTable(NewTable(testTableName))
	return client
}


