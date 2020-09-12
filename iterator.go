package dyc

import (
	"context"

	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// Iterator provides result iteration behavior
type Iterator struct {
	p request.Pagination
}

// IteratorClient is an interface for all methods utilized by iterator
type IteratorClient interface {
	QueryRequest(input *dynamodb.QueryInput) (req *request.Request, output *dynamodb.QueryOutput)
	ScanRequest(input *dynamodb.ScanInput) (req *request.Request, output *dynamodb.ScanOutput)
}

// NewIteratorFromQuery creates a new iterator from a query
func NewIteratorFromQuery(ctx context.Context, cli IteratorClient, input *dynamodb.QueryInput) *Iterator {
	p := request.Pagination{
		NewRequest: func() (*request.Request, error) {
			var inCpy *dynamodb.QueryInput
			if input != nil {
				tmp := *input
				inCpy = &tmp
			}
			req, _ := cli.QueryRequest(inCpy)
			req.SetContext(ctx)
			return req, nil
		},
	}
	return &Iterator{p: p}
}

// NewIteratorFromScan creates a new iterator from a scan
func NewIteratorFromScan(ctx context.Context, cli IteratorClient, input *dynamodb.ScanInput) *Iterator {
	p := request.Pagination{
		NewRequest: func() (*request.Request, error) {
			var inCpy *dynamodb.ScanInput
			if input != nil {
				tmp := *input
				inCpy = &tmp
			}
			req, _ := cli.ScanRequest(inCpy)
			req.SetContext(ctx)
			return req, nil
		},
	}
	return &Iterator{p: p}
}

// Next returns true if iteration can continue and false otherwise
func (i *Iterator) Next() bool {
	return i.p.Next()
}

// Value returns the current value
func (i *Iterator) Value() interface{} {
	return i.p.Page()
}

// QueryValue returns the current query output
func (i *Iterator) QueryValue() *dynamodb.QueryOutput {
	result, ok := i.Value().(*dynamodb.QueryOutput)
	if ok {
		return result
	}

	return nil
}

// ScanValue returns the current scan output
func (i *Iterator) ScanValue() *dynamodb.ScanOutput {
	result, ok := i.Value().(*dynamodb.ScanOutput)
	if ok {
		return result
	}

	return nil
}

// Err returns the last err
func (i *Iterator) Err() error {
	return i.p.Err()
}
