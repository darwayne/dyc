package dyc

import (
	"context"
	"strconv"
	"strings"
	"text/scanner"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/pkg/errors"
)

// Builder allows you to build dynamo queries in a more convenient fashion
type Builder struct {
	colsIdx         int
	valColsIdx      int
	cols            map[string]*string
	vals            map[string]*dynamodb.AttributeValue
	err             error
	filterExpresion string
	keyExpression   string
	table           string
	index           string
	limit           int
	client          *Client
}

// NewBuilder creates a new builder
func NewBuilder() *Builder {
	return &Builder{
		valColsIdx: -1,
		cols:       make(map[string]*string),
		vals:       make(map[string]*dynamodb.AttributeValue),
	}
}

// WhereKey allows you do make a key expression
// e.g WhereKey("'MyKey' = ?", "yourKey")
func (s *Builder) WhereKey(query string, vals ...interface{}) *Builder {
	if s.err != nil {
		return s
	}
	s.keyExpression, s.err = s.scan(query, vals...)
	return s
}

// Where is equivalent to a filter expression
// e.g Where("'Hey' = ? AND 'Test'.'Nested'" = ?, "yo", true)
func (s *Builder) Where(query string, vals ...interface{}) *Builder {
	if s.err != nil {
		return s
	}
	s.filterExpresion, s.err = s.scan(query, vals...)
	return s
}

// Client sets client that will be used for client operations based on built object
func (s *Builder) Client(client *Client) *Builder {
	if s.err != nil {
		return s
	}
	s.client = client
	return s
}

// QueryIterate allows you to query dynamo based on the built object.
// the fn parameter will be called as often as needed to retrieve all results
func (s *Builder) QueryIterate(ctx context.Context, fn func(output *dynamodb.QueryOutput) error) error {
	if s.err != nil {
		return s.err
	}
	if s.client == nil {
		return errors.New("client not set")
	}
	query, _ := s.ToQuery()

	return s.client.QueryIterator(ctx, &query, fn)
}

// ScanIterate allows you to query dynamo based on the built object.
// the fn parameter will be called as often as needed to retrieve all results
func (s *Builder) ScanIterate(ctx context.Context, fn func(output *dynamodb.ScanOutput) error) error {
	if s.err != nil {
		return s.err
	}
	if s.client == nil {
		return errors.New("client not set")
	}

	query, _ := s.ToScan()

	return s.client.ScanIterator(ctx, &query, fn)
}

// ParallelScanIterate allows you to do a parallel scan in dynamo based on the built object.
// the fn parameter will be called as often as needed to retrieve all results
func (s *Builder) ParallelScanIterate(ctx context.Context, workers int, fn func(output *dynamodb.ScanOutput) error) error {
	if s.err != nil {
		return s.err
	}
	if s.client == nil {
		return errors.New("client not set")
	}

	query, _ := s.ToScan()

	return s.client.ParallelScanIterator(ctx, &query, workers, fn)
}

// QueryDelete deletes all records matching the query.
// note: you must provide a function that will select the relevant Key fields needed for deletion
func (s *Builder) QueryDelete(ctx context.Context, keyFn KeyExtractor) error {
	if s.err != nil {
		return s.err
	}
	if s.client == nil {
		return errors.New("client not set")
	}

	query, _ := s.ToQuery()

	return s.client.QueryDeleter(ctx, s.table, &query, keyFn)
}

// ScanDelete deletes all records matching the scan.
// note: you must provide a function that will select the relevant Key fields needed for deletion
func (s *Builder) ScanDelete(ctx context.Context, keyFn KeyExtractor) error {
	if s.err != nil {
		return s.err
	}
	if s.client == nil {
		return errors.New("client not set")
	}

	query, _ := s.ToScan()

	return s.client.ScanDeleter(ctx, s.table, &query, keyFn)
}

// ToQuery produces a dynamodb.QueryInput value based on configured builder
func (s *Builder) ToQuery() (dynamodb.QueryInput, error) {
	if s.err != nil {
		return dynamodb.QueryInput{}, s.err
	}

	var query dynamodb.QueryInput
	if s.keyExpression != "" {
		query.KeyConditionExpression = aws.String(s.keyExpression)
	}

	if s.filterExpresion != "" {
		query.FilterExpression = aws.String(s.filterExpresion)
	}

	if len(s.cols) > 0 {
		query.ExpressionAttributeNames = s.cols
	}

	if len(s.vals) > 0 {
		query.ExpressionAttributeValues = s.vals
	}

	if s.limit > 0 {
		query.Limit = aws.Int64(int64(s.limit))
	}

	if s.index != "" {
		query.IndexName = aws.String(s.index)
	}

	if s.table != "" {
		query.TableName = aws.String(s.table)
	}

	return query, nil
}

// ToScan produces a dynamodb.ScanInput value based on configured builder
func (s *Builder) ToScan() (dynamodb.ScanInput, error) {
	if s.err != nil {
		return dynamodb.ScanInput{}, s.err
	}

	var query dynamodb.ScanInput
	if s.filterExpresion != "" {
		query.FilterExpression = aws.String(s.filterExpresion)
	}

	if len(s.cols) > 0 {
		query.ExpressionAttributeNames = s.cols
	}

	if len(s.vals) > 0 {
		query.ExpressionAttributeValues = s.vals
	}

	if s.limit > 0 {
		query.Limit = aws.Int64(int64(s.limit))
	}

	if s.index != "" {
		query.IndexName = aws.String(s.index)
	}

	if s.table != "" {
		query.TableName = aws.String(s.table)
	}

	return query, nil
}

// ToGet produces a dynamodb.GetItemInput value based on configured builder
func (s *Builder) ToGet() (dynamodb.GetItemInput, error) {
	if s.err != nil {
		return dynamodb.GetItemInput{}, s.err
	}

	var query dynamodb.GetItemInput
	if len(s.cols) > 0 {
		query.ExpressionAttributeNames = s.cols
	}

	if len(s.vals) > 0 {
		query.Key = s.vals
	}

	if s.table != "" {
		query.TableName = aws.String(s.table)
	}

	return query, nil
}

// Table sets the table name
func (s *Builder) Table(tbl string) *Builder {
	if s.err != nil {
		return s
	}
	s.table = tbl
	return s
}

// Limit sets the limit for results
func (s *Builder) Limit(limit int) *Builder {
	if s.err != nil {
		return s
	}
	s.limit = limit
	return s
}

// Index sets the index to use
func (s *Builder) Index(index string) *Builder {
	if s.err != nil {
		return s
	}
	s.index = index
	return s
}

// scan takes an input and produces a parsed version with relevant colNames and values set on the builder object
// e.g scan("'myField' = ?", 1.0)
// produces -> "#1 = :1"
// and sets #1 and :1 appropriately
func (s *Builder) scan(query string, inputs ...interface{}) (updatedQuery string, err error) {
	var builder strings.Builder
	builder.Grow(len(query))
	var sc scanner.Scanner

	sc.Init(strings.NewReader(query))
	sc.Whitespace = 0
	sc.Error = func(s *scanner.Scanner, msg string) {
	}

	start := s.valColsIdx

	for tok := sc.Scan(); tok != scanner.EOF; tok = sc.Scan() {
		val := sc.TokenText()
		switch tok {
		case -5:
			s.colsIdx++
			var c strings.Builder
			num := strconv.Itoa(s.colsIdx)
			c.Grow(1 + len(num))
			c.WriteRune('#')
			c.WriteString(num)

			col := c.String()
			value := strings.Trim(val, `'`)
			s.cols[col] = &value
			builder.WriteString(col)
		case '?':
			s.valColsIdx++
			if len(inputs) <= (s.valColsIdx - start - 1) {
				return "", errors.New("inputs don't match query")
			}
			var c strings.Builder
			num := strconv.Itoa(s.valColsIdx)
			c.Grow(1 + len(num))
			c.WriteRune(':')
			c.WriteString(num)

			col := c.String()
			attr, err := typeToAttributeVal(inputs[s.valColsIdx-start-1])
			if err != nil {
				return "", err
			}
			s.vals[col] = attr
			builder.WriteString(col)
		default:
			builder.WriteString(sc.TokenText())
		}
	}

	return builder.String(), nil
}

func typeToAttributeVal(raw interface{}) (*dynamodb.AttributeValue, error) {
	switch v := raw.(type) {
	case string:
		return &dynamodb.AttributeValue{S: aws.String(v)}, nil
	case []string:
		return &dynamodb.AttributeValue{SS: aws.StringSlice(v)}, nil
	case int:
		return &dynamodb.AttributeValue{N: aws.String(strconv.Itoa(v))}, nil
	case float64:
		return &dynamodb.AttributeValue{N: aws.String(
			strconv.FormatFloat(v, 'f', -1, 64))}, nil
	case []byte:
		return &dynamodb.AttributeValue{B: v}, nil
	case [][]byte:
		return &dynamodb.AttributeValue{BS: v}, nil
	case bool:
		return &dynamodb.AttributeValue{BOOL: aws.Bool(v)}, nil
	case dynamodb.AttributeValue:
		return &v, nil
	case *dynamodb.AttributeValue:
		return v, nil
	}

	return nil, errors.New("unsupported type")
}
