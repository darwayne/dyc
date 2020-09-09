package dyc

import "errors"

var (
	ErrClientNotSet    = errors.New("client not set")
	ErrBadKeyType      = errors.New("strings are the only type of keys supported")
	ErrKeyRequired     = errors.New("key not set")
	ErrBadKeyParams    = errors.New("expected an even amount of additionalKVs parameters")
	ErrUnsupportedType = errors.New("unsupported type")
	ErrQueryMisMatch   = errors.New("inputs don't match query")
)
