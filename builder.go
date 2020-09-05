package dyc

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"text/scanner"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/pkg/errors"
)

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

func NewBuilder() *Builder {
	return &Builder{
		valColsIdx: -1,
		cols:       make(map[string]*string),
		vals:       make(map[string]*dynamodb.AttributeValue),
	}
}

// WhereKey is equivalent to key condition
func (s *Builder) WhereKey(query string, vals ...interface{}) *Builder {
	if s.err != nil {
		return s
	}
	s.keyExpression, s.err = s.scan(query, vals...)
	return s
}

// Where is equivalent to a filter expression
func (s *Builder) Where(query string, vals ...interface{}) *Builder {
	if s.err != nil {
		return s
	}
	s.filterExpresion, s.err = s.scan(query, vals...)
	return s
}

func (s *Builder) Client(client *Client) *Builder {
	if s.err != nil {
		return s
	}
	s.client = client
	return s
}

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

func (s *Builder) Table(tbl string) *Builder {
	if s.err != nil {
		return s
	}
	s.table = tbl
	return s
}

func (s *Builder) Limit(limit int) *Builder {
	if s.err != nil {
		return s
	}
	s.limit = limit
	return s
}

func (s *Builder) Index(index string) *Builder {
	if s.err != nil {
		return s
	}
	s.index = index
	return s
}

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
			s.cols[col] = &val
			builder.WriteString(col)
		case '?':
			s.valColsIdx++
			if len(inputs) <= (s.valColsIdx - start) {
				return "", errors.New("inputs don't match query")
			}
			var c strings.Builder
			num := strconv.Itoa(s.valColsIdx)
			c.Grow(1 + len(num))
			c.WriteRune(':')
			c.WriteString(num)

			col := c.String()
			attr, err := typeToAttributeVal(inputs[s.valColsIdx-start])
			if err != nil {
				return "", err
			}
			s.vals[col] = attr
			builder.WriteString(col)
		default:
			fmt.Println(tok, sc.TokenText())
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
