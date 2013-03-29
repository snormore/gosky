package sky

//------------------------------------------------------------------------------
//
// Typedefs
//
//------------------------------------------------------------------------------

// An error generated from the Sky server.
type Error struct {
	message string
}


//------------------------------------------------------------------------------
//
// Constructor
//
//------------------------------------------------------------------------------

// NewError creates a new Sky error object.
func NewError(message string) *Error {
	return &Error{message:message}
}


//------------------------------------------------------------------------------
//
// Methods
//
//------------------------------------------------------------------------------

// The error message.
func (e *Error) Error() string {
	return e.message
}
