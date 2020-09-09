package dyc

import "errors"

var (
	ClientNotSetErr    = errors.New("client not set")
	BadKeyTypeErr      = errors.New("strings are the only type of keys supported")
	KeyRequiredErr     = errors.New("key not set")
	BadKeyParamsErr    = errors.New("expected an even amount of additionalKVs parameters")
	UnsupportedTypeErr = errors.New("unsupported type")
	QueryMisMatchErr   = errors.New("inputs don't match query")
)
