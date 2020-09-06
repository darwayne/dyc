package dyc

import (
	"context"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/pkg/errors"
)

// Client is a wrapper around the dynamodb SDK that provides useful behavior
// such as iteration, processing unprocessed items and more
type Client struct {
	*dynamodb.DynamoDB
}

func NewClient(db *dynamodb.DynamoDB) *Client {
	return &Client{DynamoDB: db}
}

// BatchWriter batch writes an array of write requests to a table
func (c *Client) BatchWriter(ctx context.Context, tableName string, requests ...*dynamodb.WriteRequest) (int, error) {
	totalWritten := 0
	chunks := c.ChunkWriteRequests(requests)
	for _, chunk := range chunks {
		out, err := c.DynamoDB.BatchWriteItemWithContext(ctx, &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]*dynamodb.WriteRequest{
				tableName: chunk,
			},
		})

		if err != nil {
			return totalWritten, err
		}

		totalWritten += len(chunk) - len(out.UnprocessedItems)

		if len(out.UnprocessedItems) > 0 {
			for table, reqs := range out.UnprocessedItems {
				total, err := c.BatchWriter(ctx, table, reqs...)
				totalWritten += total
				if err != nil {
					return totalWritten, err
				}
			}
		}

	}

	return totalWritten, nil
}

// Builder produces a builder configured with the current client
func (c *Client) Builder() *Builder {
	return NewBuilder().Client(c)
}

// QueryIterator iterates all results of a query
func (c *Client) QueryIterator(ctx context.Context, input *dynamodb.QueryInput, fn func(output *dynamodb.QueryOutput) error) error {
	var pageError error
	err := c.DynamoDB.QueryPagesWithContext(ctx, input, func(output *dynamodb.QueryOutput, b bool) bool {
		pageError = fn(output)
		return pageError == nil
	})

	if err != nil {
		return err
	}

	if pageError != nil {
		return pageError
	}

	return nil
}

// ParallelScanIterator is a thread safe method that performs a parallel scan on dynamodb utilizing the configured amount of workers
func (c *Client) ParallelScanIterator(ctx context.Context, input *dynamodb.ScanInput, workers int, fn func(output *dynamodb.ScanOutput) error) error {
	var mu sync.Mutex
	var wg sync.WaitGroup
	errChan := make(chan error, 1)
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	wg.Add(workers)

	input.TotalSegments = aws.Int64(int64(workers))

	worker := func(idx int) {
		defer wg.Done()
		arg := *input
		arg.Segment = aws.Int64(int64(idx))
		err := c.ScanIterator(ctx, &arg, func(out *dynamodb.ScanOutput) error {
			mu.Lock()
			e := fn(out)
			mu.Unlock()

			select {
			case <-workerCtx.Done():
				return workerCtx.Err()
			default:

			}

			if e != nil {
				select {
				case <-workerCtx.Done():
					return workerCtx.Err()
				case errChan <- e:
				default:
					return errors.New("exit early")
				}
			}

			return nil
		})

		if err != nil {
			select {
			case <-workerCtx.Done():
				return
			case errChan <- err:
			default:
				return
			}
		}
	}

	for i := 0; i < workers; i++ {
		go worker(i)
	}

	go func() {
		wg.Wait()
		cancel()
	}()

	select {
	case err := <-errChan:
		return err
	case <-workerCtx.Done():

	}

	return nil
}

// ScanIterator iterates all results of a scan
func (c *Client) ScanIterator(ctx context.Context, input *dynamodb.ScanInput, fn func(output *dynamodb.ScanOutput) error) error {
	var pageError error
	err := c.DynamoDB.ScanPagesWithContext(ctx, input, func(output *dynamodb.ScanOutput, b bool) bool {
		pageError = fn(output)
		return pageError == nil
	})

	if err != nil {
		return err
	}

	if pageError != nil {
		return pageError
	}

	return nil
}

// QueryDeleter deletes all records that match the query
func (c *Client) QueryDeleter(ctx context.Context, table string, input *dynamodb.QueryInput, keyFn KeyExtractor) error {
	err := c.QueryIterator(ctx, input, func(out *dynamodb.QueryOutput) error {
		requests := make([]*dynamodb.WriteRequest, 0, len(out.Items))
		for _, attrs := range out.Items {
			requests = append(requests, &dynamodb.WriteRequest{
				DeleteRequest: &dynamodb.DeleteRequest{
					Key: keyFn(attrs),
				},
			})
		}

		if _, err := c.BatchWriter(ctx, table, requests...); err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return err
	}

	return nil
}

// ScanDelete deletes all records that match the scan query
func (c *Client) ScanDeleter(ctx context.Context, table string, input *dynamodb.ScanInput, keyFn KeyExtractor) error {
	err := c.ScanIterator(ctx, input, func(out *dynamodb.ScanOutput) error {
		requests := make([]*dynamodb.WriteRequest, 0, len(out.Items))
		for _, attrs := range out.Items {
			requests = append(requests, &dynamodb.WriteRequest{
				DeleteRequest: &dynamodb.DeleteRequest{
					Key: keyFn(attrs),
				},
			})
		}

		if _, err := c.BatchWriter(ctx, table, requests...); err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return err
	}

	return nil
}

// BatchGetIterator retrieves all items from the batch get input
func (c *Client) BatchGetIterator(ctx context.Context, input *dynamodb.BatchGetItemInput, fn func(output *dynamodb.GetItemOutput) error) error {
	var pageError error
	err := c.DynamoDB.BatchGetItemPagesWithContext(ctx, input, func(output *dynamodb.BatchGetItemOutput, b bool) bool {
		var capacity *dynamodb.ConsumedCapacity = nil
		if len(output.ConsumedCapacity) > 0 {
			capacity = output.ConsumedCapacity[0]
		}
		for _, results := range output.Responses {
			for _, raw := range results {
				pageError = fn(&dynamodb.GetItemOutput{
					Item:             raw,
					ConsumedCapacity: capacity,
				})
				if pageError != nil {
					return false
				}
			}
		}

		for tbl, unprocessed := range output.UnprocessedKeys {
			pageError = c.BatchGetIterator(ctx, c.ToBatchGetItemInput(tbl, unprocessed.Keys), fn)
			if pageError != nil {
				return false
			}
		}

		return true
	})

	if err != nil {
		return err
	}

	if pageError != nil {
		return pageError
	}

	return nil
}

// ExTractFields extracts fields from a map of dynamo attribute values
func (c *Client) ExtractFields(data map[string]*dynamodb.AttributeValue, fields ...string) map[string]*dynamodb.AttributeValue {
	return extractFields(data, fields...)
}

func (c *Client) ToBatchGetItemInput(tableName string, req []map[string]*dynamodb.AttributeValue) *dynamodb.BatchGetItemInput {
	return &dynamodb.BatchGetItemInput{RequestItems: map[string]*dynamodb.KeysAndAttributes{
		tableName: {
			Keys: req,
		},
	}}
}

// ChunkWriteRequests chunks write requests into batches of 25 (the current maximum size in AWS)
func (c *Client) ChunkWriteRequests(requests []*dynamodb.WriteRequest) [][]*dynamodb.WriteRequest {
	chunkSize := 25
	results := make([][]*dynamodb.WriteRequest, 0, len(requests)/chunkSize)

	total := len(requests)
	for i := 0; i < total; i += chunkSize {
		end := i + chunkSize
		if end > total {
			end = total
		}
		results = append(results, requests[i:end])
	}

	return results
}
