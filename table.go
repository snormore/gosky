package sky

//------------------------------------------------------------------------------
//
// Typedefs
//
//------------------------------------------------------------------------------

// A Table is a container for objects and events.
type Table struct {
	client *Client
	Name string `json:"name"`
}

//------------------------------------------------------------------------------
//
// Constructor
//
//------------------------------------------------------------------------------

func NewTable(name string) *Table {
	return &Table{
		Name:    name,
	}
}