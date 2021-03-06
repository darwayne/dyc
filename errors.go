package dyc

import "errors"

var (
	// ErrClientNotSet occurs if you try to make a client call from a builder without setting the client
	ErrClientNotSet = errors.New("client not set")
	// ErrBadKeyType occurs if you try to provide a non string key name
	ErrBadKeyType = errors.New("strings are the only type of keys supported")
	// ErrKeyRequired occurs if key is required for the given operation
	ErrKeyRequired = errors.New("key not set")
	// ErrBadKeyParams occurs if k/v params don't line up
	ErrBadKeyParams = errors.New("expected an even amount of additionalKVs parameters")
	// ErrUnsupportedType occurs if you are trying to add an unsupported type as an input for a query
	ErrUnsupportedType = errors.New("unsupported type")
	// ErrQueryMisMatch occurs if the number of ? don't line up with the given inputs for a query
	ErrQueryMisMatch = errors.New("inputs don't match query")
	// ErrNotSlice occurs if a non slice type is provided as a value for any of the IN builder query functions
	ErrNotSlice = errors.New("provided value is not a slice")
	// ErrNotPointer occurs if a non pointer type is provided to the Result method of the builder type
	ErrNotPointer = errors.New("provided result type is not a slice")
)
