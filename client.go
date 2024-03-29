package dyc

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/pkg/errors"
)

// Client is a wrapper around the dynamodb SDK that provides useful behavior
// such as iteration, processing unprocessed items and more
type Client struct {
	*dynamodb.DynamoDB
}

// NewClient creates a new dyc client
func NewClient(db *dynamodb.DynamoDB) *Client {
	return &Client{DynamoDB: db}
}

// BatchPut allows you to put a batch of items to a table
// items will me converted to a marshal map
func (c *Client) BatchPut(ctx context.Context, tableName string, items ...interface{}) (int, error) {
	arr := make([]interface{}, 0, len(items))
	for _, a := range items {
		vals := sliceToValues(a)
		if vals == nil {
			arr = append(arr, a)
		} else {
			arr = append(arr, vals...)
		}
	}
	requests := make([]*dynamodb.WriteRequest, 0, len(arr))
	for _, a := range arr {
		data, err := dynamodbattribute.MarshalMap(a)
		if err != nil {
			return 0, err
		}
		requests = append(requests, &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: data,
			},
		})
	}
	return c.BatchWriter(ctx, tableName, requests...)
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
	//TODO: clean this up
	in2 := *input
	hasLimit := input.Limit != nil
	var limit int
	if hasLimit {
		limit = int(*input.Limit)
		in2.Limit = nil
	}
	seen := 0
	var pageError error
	err := c.DynamoDB.QueryPagesWithContext(ctx, &in2, func(output *dynamodb.QueryOutput, b bool) bool {
		if hasLimit {
			var added, broke bool
			var items []map[string]*dynamodb.AttributeValue
			for _, i := range output.Items {
				seen++
				if seen > limit {
					broke = true
					break
				}
				added = true
				items = append(items, i)
			}
			if seen > 0 && !added && broke {
				return false
			}
			output.Items = items
		}
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

// QueryIteratorV2 iterates all results of a query respecting relevant keys
func (c *Client) QueryIteratorV2(ctx context.Context, input *dynamodb.QueryInput, keys []string, fn func(output *dynamodb.QueryOutput) error) error {
	modifier := limitModifier(&input.Limit)
	var pageError error
	err := c.DynamoDB.QueryPagesWithContext(ctx, input, func(output *dynamodb.QueryOutput, b bool) bool {
		if len(output.Items) == 0 {
			return true
		}
		trimmed, exitEarly := modifier(&output.Items)
		if !exitEarly {
			return false
		}
		totalItems := len(output.Items)
		lastIDX := totalItems - 1
		if totalItems > 0 && trimmed {
			output.SetLastEvaluatedKey(extractFields(output.Items[lastIDX], keys...))
		}
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

func (c *Client) onCopyData(ctx context.Context, dst string, working *int64, errChan chan error, data map[string]*dynamodb.AttributeValue) {
	atomic.AddInt64(working, 1)
	defer func() {
		atomic.AddInt64(working, -1)
	}()
	_, err := c.PutItemWithContext(ctx, &dynamodb.PutItemInput{
		Item:      data,
		TableName: &dst,
	})

	if err != nil {
		select {
		case <-ctx.Done():
			return
		case errChan <- err:
		}
	}

}
func (c *Client) copyTableWorker(ctx context.Context, dst string, readComplete chan struct{}, dataChan chan map[string]*dynamodb.AttributeValue, working *int64, wg *sync.WaitGroup, errChan chan error) {
	defer wg.Done()
	for {
		select {
		case <-time.After(10 * time.Second):
			select {
			case <-readComplete:
				if atomic.LoadInt64(working) == 0 && len(dataChan) == 0 {
					return
				}
			default:
				continue
			}
		case <-ctx.Done():
			return
		case data, open := <-dataChan:
			if !open {
				return
			}

			c.onCopyData(ctx, dst, working, errChan, data)
		}
	}
}

// CopyTable copies all data in source to the existing destination table using
func (c *Client) CopyTable(parentCtx context.Context, dst string, src string, workers int, onError func(err error, cancelFunc context.CancelFunc)) error {
	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()

	errChan := make(chan error, workers)
	readComplete := make(chan struct{})
	dataChan := make(chan map[string]*dynamodb.AttributeValue, workers)
	var wg sync.WaitGroup
	wg.Add(1 + workers)
	var working int64

	for i := 0; i < workers; i++ {
		go c.copyTableWorker(ctx, dst, readComplete, dataChan, &working, &wg, errChan)
	}

	go func() {
		defer wg.Done()
		err := c.ParallelScanIterator(ctx, &dynamodb.ScanInput{
			TableName: aws.String(src),
		}, workers, func(output *dynamodb.ScanOutput) error {
			for _, item := range output.Items {
				select {
				case dataChan <- item:
				case <-ctx.Done():
					return ctx.Err()
				}
			}

			return nil
		}, true)

		close(dataChan)
		close(readComplete)
		if err != nil {
			select {
			case <-ctx.Done():
			case errChan <- err:
			}
		}
	}()

	complete := make(chan struct{})
	go func() {
		wg.Wait()
		close(complete)
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-errChan:
			if onError == nil {
				cancel()
				<-complete
				return err
			}

			onError(err, cancel)
		case <-complete:
			return nil
		}
	}
}

func (c *Client) parallelScanWorker(ctx context.Context, idx int, arg dynamodb.ScanInput, wg *sync.WaitGroup, errChan chan error, mu *sync.Mutex, noLock bool, fn func(output *dynamodb.ScanOutput) error) {
	defer wg.Done()
	arg.Segment = aws.Int64(int64(idx))
	err := c.ScanIterator(ctx, &arg, func(out *dynamodb.ScanOutput) error {
		var e error
		if noLock {
			e = fn(out)
		} else {
			mu.Lock()
			e = fn(out)
			mu.Unlock()
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:

		}

		if e != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case errChan <- e:
			default:
				return errors.New("exit early")
			}
		}

		return nil
	})

	if err != nil {
		select {
		case <-ctx.Done():
			return
		case errChan <- err:
		default:
			return
		}
	}
}

// ParallelScanIterator is a thread safe method that performs a parallel scan on dynamodb utilizing the configured amount of workers
func (c *Client) ParallelScanIterator(ctx context.Context, input *dynamodb.ScanInput, workers int, fn func(output *dynamodb.ScanOutput) error, noLock bool) error {
	var mu sync.Mutex
	var wg sync.WaitGroup
	errChan := make(chan error, 1)
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	wg.Add(workers)

	input.TotalSegments = aws.Int64(int64(workers))

	for i := 0; i < workers; i++ {
		go c.parallelScanWorker(workerCtx, i, *input, &wg, errChan, &mu, noLock, fn)
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
	//TODO: clean this up
	in2 := *input
	hasLimit := input.Limit != nil
	var limit int
	if hasLimit {
		limit = int(*input.Limit)
		in2.Limit = nil
	}
	seen := 0
	var pageError error
	err := c.DynamoDB.ScanPagesWithContext(ctx, &in2, func(output *dynamodb.ScanOutput, b bool) bool {
		if hasLimit {
			var added, broke bool
			var items []map[string]*dynamodb.AttributeValue
			for _, i := range output.Items {
				seen++
				if seen > limit {
					broke = true
					break
				}
				added = true
				items = append(items, i)
			}
			if seen > 0 && !added && broke {
				return false
			}
			output.Items = items
		}
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

// ScanIteratorV2 iterates all results of a scan respecting keys
func (c *Client) ScanIteratorV2(ctx context.Context, input *dynamodb.ScanInput, keys []string, fn func(output *dynamodb.ScanOutput) error) error {
	modifier := limitModifier(&input.Limit)
	var pageError error
	err := c.DynamoDB.ScanPagesWithContext(ctx, input, func(output *dynamodb.ScanOutput, b bool) bool {
		if len(output.Items) == 0 {
			return true
		}
		trimmed, exitEarly := modifier(&output.Items)
		if !exitEarly {
			return false
		}
		totalItems := len(output.Items)
		lastIDX := totalItems - 1
		if totalItems > 0 && trimmed {
			output.SetLastEvaluatedKey(extractFields(output.Items[lastIDX], keys...))
		}
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

// ScanCount counts all records matching the scan query
func (c *Client) ScanCount(ctx context.Context, input *dynamodb.ScanInput) (int64, error) {
	i := *input
	i.Select = aws.String(dynamodb.SelectCount)
	var total int64
	c.ScanIterator(ctx, &i, func(output *dynamodb.ScanOutput) error {
		if output.Count == nil {
			return errors.New("count nil")
		}
		atomic.AddInt64(&total, *output.Count)
		return nil
	})
	return total, nil
}

// QueryDeleter deletes all records that match the query
func (c *Client) QueryDeleter(ctx context.Context, table string, input *dynamodb.QueryInput, keys []string) error {
	keyFn := FieldsExtractor(keys...)
	err := c.QueryIteratorV2(ctx, input, keys, func(out *dynamodb.QueryOutput) error {
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

// ScanDeleter deletes all records that match the scan query
func (c *Client) ScanDeleter(ctx context.Context, table string, input *dynamodb.ScanInput, keys []string) error {
	keyFn := FieldsExtractor(keys...)
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

// ExtractFields extracts fields from a map of dynamo attribute values
func (c *Client) ExtractFields(data map[string]*dynamodb.AttributeValue, fields ...string) map[string]*dynamodb.AttributeValue {
	return extractFields(data, fields...)
}

// ToBatchGetItemInput converts an array of mapped dynamo attributes to a batch get item input
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

// limitModifier utilizes the dynamo limit input and treats it as page size. if limit is set it will be unset
func limitModifier(inputLimit **int64) func(maps *Maps) (trimmed, exitEarly bool) {
	hasLimit := *inputLimit != nil
	var limit int
	if hasLimit {
		limit = int(**inputLimit)
		*inputLimit = nil
	}
	seen := 0

	return func(outputItems *Maps) (bool, bool) {
		if hasLimit {
			var added, broke bool
			var items []map[string]*dynamodb.AttributeValue
			for _, i := range *outputItems {
				seen++
				if seen > limit {
					broke = true
					break
				}

				added = true
				items = append(items, i)
			}
			if seen > 0 && !added && broke {
				return broke, false
			}
			*outputItems = items

			return broke, true
		}

		return false, true
	}
}
