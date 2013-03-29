package sky

//------------------------------------------------------------------------------
//
// Constants
//
//------------------------------------------------------------------------------

const (
	String  = "string"
	Integer = "integer"
	Float   = "float"
	Boolean = "boolean"
	Factor  = "factor"
)

//------------------------------------------------------------------------------
//
// Typedefs
//
//------------------------------------------------------------------------------

// A Property is part of the table's schema.
type Property struct {
	Id        int    `json:"id"`
	Name      string `json:"name"`
	Transient bool   `json:"transient"`
	DataType  string `json:"dataType"`
}

//------------------------------------------------------------------------------
//
// Constructor
//
//------------------------------------------------------------------------------

func NewProperty(name string, transient bool, dataType string) *Property {
	return &Property{
		Name:      name,
		Transient: transient,
		DataType:  dataType,
	}
}
