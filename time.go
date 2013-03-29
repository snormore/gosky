package sky

import (
	"time"
)

//------------------------------------------------------------------------------
//
// Functions
//
//------------------------------------------------------------------------------

// Parses an ISO8601 timestamp with or without fractional seconds.
func ParseTimestamp(str string) (time.Time, error) {
	if timestamp, err := time.Parse(time.RFC3339Nano, str); err == nil {
		return timestamp, nil
	}
	return time.Parse(time.RFC3339, str)
}

// Formats a time into ISO8601 format with fractional seconds.
func FormatTimestamp(timestamp time.Time) string {
	return timestamp.UTC().Format(time.RFC3339Nano)
}
