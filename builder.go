package dyc

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"text/scanner"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

// Builder allows you to build dynamo queries in a more convenient fashion
type Builder struct {
	colsIdx             int
	valColsIdx          int
	selectedFields      string
	cols                map[string]*string
	vals                map[string]*dynamodb.AttributeValue
	keys                map[string]*dynamodb.AttributeValue
	err                 error
	filterExpresion     string
	updateExpression    string
	keyExpression       string
	conditionExpression string
	table               string
	index               string
	limit               int
	ascending           *bool
	consistent          *bool
	client              *Client
	result              interface{}
}

// NewBuilder creates a new builder
func NewBuilder() *Builder {
	return &Builder{
		valColsIdx: -1,
		cols:       make(map[string]*string),
		vals:       make(map[string]*dynamodb.AttributeValue),
		keys:       make(map[string]*dynamodb.AttributeValue),
	}
}

// Key allows you to set the key for a given item
// e.g Key("PK", "hello", "SK", "there")
func (s *Builder) Key(keyName string, value interface{}, additionalKVs ...interface{}) *Builder {
	return s.update(func() {
		var firstVal *dynamodb.AttributeValue
		firstVal, s.err = typeToAttributeVal(value)
		if s.err != nil {
			return
		}

		s.keys[keyName] = firstVal
		if len(additionalKVs) > 0 && (len(additionalKVs)%2) != 0 {
			s.err = ErrBadKeyParams
			return
		}

		for i := 0; i < len(additionalKVs); i += 2 {
			k, ok := additionalKVs[i].(string)
			if !ok {
				s.err = ErrBadKeyType
				return
			}

			var val *dynamodb.AttributeValue
			val, s.err = typeToAttributeVal(additionalKVs[i+1])
			if s.err != nil {
				return
			}

			s.keys[k] = val
		}
	})
}

// Condition allows you do make a condition expression.
// e.g Condition("'MyKey' = ?", "yourKey")
// note: calling this multiple times combines conditions with an AND
func (s *Builder) Condition(query string, vals ...interface{}) *Builder {
	return s.update(func() {
		s.addExpression(&s.conditionExpression, "AND", query, vals...)
	})
}

// OrCondition allows you do make an OR if you have multiple conditions
// e.g Condition("'MyKey' = ?", "yourKey")
// note: calling this multiple times combines conditions with an OR
func (s *Builder) OrCondition(query string, vals ...interface{}) *Builder {
	return s.update(func() {
		s.addExpression(&s.conditionExpression, "OR", query, vals...)
	})
}

// WhereKey allows you do make a key expression
// e.g WhereKey("'MyKey' = ?", "yourKey")
// note: calling this multiple times combines conditions with an AND
func (s *Builder) WhereKey(query string, vals ...interface{}) *Builder {
	return s.update(func() {
		s.addExpression(&s.keyExpression, "AND", query, vals...)
	})
}

// Where is equivalent to a filter expression
// e.g Where("'Hey' = ? AND 'Test'.'Nested'" = ?, "yo", true)
// note: calling this multiple times combines conditions with an AND
func (s *Builder) Where(query string, vals ...interface{}) *Builder {
	return s.update(func() {
		s.addExpression(&s.filterExpresion, "AND", query, vals...)
	})
}

// Update is equivalent to an update expression
// e.g Update("'Hey' = ? AND 'Test'.'Nested'" = ?, "yo", true)
// note: calling this multiple times combines conditions with an AND
func (s *Builder) Update(query string, vals ...interface{}) *Builder {
	return s.update(func() {
		s.addExpression(&s.updateExpression, "AND", query, vals...)
	})
}

// OrWhere is equivalent to a filter expression with an OR
// e.g Where("'Hey' = ? AND 'Test'.'Nested'" = ?, "yo", true).OrWhere("'Foo' = ?", "bar")
func (s *Builder) OrWhere(query string, vals ...interface{}) *Builder {
	return s.update(func() {
		s.addExpression(&s.filterExpresion, "OR", query, vals...)
	})
}

// SelectFields allows you to select which fields to pull back from dynamo
func (s *Builder) SelectFields(fields ...string) *Builder {
	return s.update(func() {
		query := strings.Join(fields, ",")
		s.selectedFields, s.err = s.scan(query)
	})
}

func (s *Builder) addExpression(expression *string, separator, query string, vals ...interface{}) {
	var result string
	result, s.err = s.scan(query, vals...)
	result = "(" + result + ")"
	if *expression == "" {
		*expression = result
	} else {
		*expression += " " + separator + " " + result
	}
}

// IN lets you build an IN filter expression
// e.g IN(`COL_NAME_HERE`, 1,2,3,4,5)
// note: if you have an existing filter expression this will be prefixed with an AND
func (s *Builder) IN(column string, vals ...interface{}) *Builder {
	return s.update(func() {
		s.Where(s.toInQuery(column, vals...), vals...)
	})
}

// INSlice lets you build an IN filter expression from a slice
// e.g INSlice(`COL_NAME_HERE`, []int{1,2,3,4,5})
// note: if you have an existing filter expression this will be prefixed with an AND
func (s *Builder) INSlice(column string, val interface{}) *Builder {
	return s.update(func() {
		result := sliceToValues(val)
		if result == nil {
			s.err = ErrNotSlice
			return
		}
		s.IN(column, result...)
	})
}

// OrIN lets you build an IN filter expression
// e.g IN(`COL_NAME_HERE`, 1,2,3,4,5)
// note: if you have an existing filter expression this will be prefixed with an OR
func (s *Builder) OrIN(column string, vals ...interface{}) *Builder {
	return s.update(func() {
		s.OrWhere(s.toInQuery(column, vals...), vals...)
	})
}

// OrINSlice lets you build an IN filter expression from a slice
// e.g INSlice(`COL_NAME_HERE`, []int{1,2,3,4,5})
// note: if you have an existing filter expression this will be prefixed with an OR
func (s *Builder) OrINSlice(column string, val interface{}) *Builder {
	return s.update(func() {
		result := sliceToValues(val)
		if result == nil {
			s.err = ErrNotSlice
			return
		}
		s.OrIN(column, result...)
	})
}

func (s *Builder) toInQuery(column string, vals ...interface{}) string {
	var builder strings.Builder
	builder.WriteString(column)
	builder.WriteString(" IN(")
	lastIdx := len(vals) - 1
	for idx := range vals {
		builder.WriteString("?")
		if idx != lastIdx {
			builder.WriteString(",")
		}
	}

	builder.WriteString(")")

	return builder.String()
}

// Result decodes the final result into the provided variable. Provided variable must be a pointer
func (s *Builder) Result(val interface{}) *Builder {
	s.result = val

	return s
}

func (s *Builder) update(fn func()) *Builder {
	if s.err != nil {
		return s
	}

	fn()

	return s
}

// Client sets client that will be used for client operations based on built object
func (s *Builder) Client(client *Client) *Builder {
	return s.update(func() {
		s.client = client
	})
}

// Sort sets sort as either ascending or descending
func (s *Builder) Sort(ascending bool) *Builder {
	return s.update(func() {
		s.ascending = aws.Bool(ascending)
	})
}

// ConsistentRead sets the consistent read flag
func (s *Builder) ConsistentRead(consistent bool) *Builder {
	return s.update(func() {
		s.consistent = aws.Bool(consistent)
	})
}

// GetItem builds and runs a query using info in key and table
func (s *Builder) GetItem(ctx context.Context) (*dynamodb.GetItemOutput, error) {
	if s.err != nil {
		return nil, s.err
	}
	if s.client == nil {
		return nil, ErrClientNotSet
	}

	input, _ := s.ToGet()
	output, err := s.client.GetItemWithContext(ctx, &input)

	return output, s.parseResult(input, err)
}

func (s *Builder) parseResult(result interface{}, errs ...error) error {
	if len(errs) > 0 && errs[0] != nil {
		return errs[0]
	}

	var err error
	if s.result == nil {
		return nil
	}

	rv := reflect.ValueOf(s.result)
	if rv.Kind() != reflect.Ptr {
		return ErrNotPointer
	}

	rv = rv.Elem()
	switch rv.Kind() {
	case reflect.Array, reflect.Slice:
		raw, ok := result.([]map[string]*dynamodb.AttributeValue)
		if !ok {
			return ErrUnsupportedType
		}
		err = dynamodbattribute.UnmarshalListOfMaps(raw, s.result)
	default:
		raw, ok := result.(map[string]*dynamodb.AttributeValue)
		if !ok {
			return ErrUnsupportedType
		}
		err = dynamodbattribute.UnmarshalMap(raw, s.result)
	}

	return err
}

// PutItem inserts the provided data and marshal maps it using the aws sdk
func (s *Builder) PutItem(ctx context.Context, data interface{}) (*dynamodb.PutItemOutput, error) {
	if s.err != nil {
		return nil, s.err
	}
	if s.client == nil {
		return nil, ErrClientNotSet
	}

	input, err := s.ToPut(data)
	if err != nil {
		return nil, err
	}
	output, err := s.client.PutItemWithContext(ctx, &input)

	return output, err
}

// UpdateItem builds and runs an update query
func (s *Builder) UpdateItem(ctx context.Context) (*dynamodb.UpdateItemOutput, error) {
	if s.err != nil {
		return nil, s.err
	}
	if s.client == nil {
		return nil, ErrClientNotSet
	}

	input, _ := s.ToUpdate()
	output, err := s.client.UpdateItemWithContext(ctx, &input)

	return output, err
}

// DeleteItem deletes a single item utilizing data set via Table, Keys and Condition method calls
func (s *Builder) DeleteItem(ctx context.Context) (*dynamodb.DeleteItemOutput, error) {
	input, err := s.ToDelete()
	if err != nil {
		return nil, err
	}

	if s.client == nil {
		return nil, ErrClientNotSet
	}

	return s.client.DeleteItemWithContext(ctx, &input)
}

// QueryIterate allows you to query dynamo based on the built object.
// the fn parameter will be called as often as needed to retrieve all results
func (s *Builder) QueryIterate(ctx context.Context, fn func(output *dynamodb.QueryOutput) error) error {
	if s.err != nil {
		return s.err
	}
	if s.client == nil {
		return ErrClientNotSet
	}
	query, err := s.ToQuery()
	if err != nil {
		return err
	}

	return s.client.QueryIterator(ctx, &query, fn)
}

// QueryAll returns an all results matching the built query
func (s *Builder) QueryAll(ctx context.Context) ([]map[string]*dynamodb.AttributeValue, error) {
	if s.err != nil {
		return nil, s.err
	}
	if s.client == nil {
		return nil, ErrClientNotSet
	}
	query, _ := s.ToQuery()
	var results []map[string]*dynamodb.AttributeValue
	err := s.client.QueryIterator(ctx, &query, func(output *dynamodb.QueryOutput) error {
		results = append(results, output.Items...)

		return nil
	})

	return results, s.parseResult(results, err)
}

// QuerySingle returns a single result matching the built query
func (s *Builder) QuerySingle(ctx context.Context) (map[string]*dynamodb.AttributeValue, error) {
	if s.err != nil {
		return nil, s.err
	}
	if s.client == nil {
		return nil, ErrClientNotSet
	}
	query, _ := s.ToQuery()
	query.Limit = aws.Int64(1)

	earlyExit := errors.New("early exit")
	var result map[string]*dynamodb.AttributeValue = nil
	err := s.client.QueryIterator(ctx, &query, func(output *dynamodb.QueryOutput) error {
		if len(output.Items) > 0 {
			result = output.Items[0]
			return earlyExit
		}

		return nil
	})

	if err == earlyExit {
		err = nil
	}

	return result, s.parseResult(result, err)
}

// ScanIterate allows you to query dynamo based on the built object.
// the fn parameter will be called as often as needed to retrieve all results
func (s *Builder) ScanIterate(ctx context.Context, fn func(output *dynamodb.ScanOutput) error) error {
	if s.err != nil {
		return s.err
	}
	if s.client == nil {
		return ErrClientNotSet
	}

	query, _ := s.ToScan()

	return s.client.ScanIterator(ctx, &query, fn)
}

// ParallelScanIterate allows you to do a parallel scan in dynamo based on the built object.
// the fn parameter will be called as often as needed to retrieve all results
func (s *Builder) ParallelScanIterate(ctx context.Context, workers int, fn func(output *dynamodb.ScanOutput) error, unsafe bool) error {
	if s.err != nil {
		return s.err
	}
	if s.client == nil {
		return ErrClientNotSet
	}

	query, _ := s.ToScan()

	return s.client.ParallelScanIterator(ctx, &query, workers, fn, unsafe)
}

// QueryDelete deletes all records matching the query.
// note: you must provide a function that will select the relevant Key fields needed for deletion
func (s *Builder) QueryDelete(ctx context.Context, keyFn KeyExtractor) error {
	if s.err != nil {
		return s.err
	}
	if s.client == nil {
		return ErrClientNotSet
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
		return ErrClientNotSet
	}

	query, _ := s.ToScan()

	return s.client.ScanDeleter(ctx, s.table, &query, keyFn)
}

// ToDelete produces a dynamodb.DeleteItemInput value based on configured builder
func (s *Builder) ToDelete() (dynamodb.DeleteItemInput, error) {
	if s.err != nil {
		return dynamodb.DeleteItemInput{}, s.err
	}

	if len(s.keys) == 0 {
		return dynamodb.DeleteItemInput{}, ErrKeyRequired
	}

	var request dynamodb.DeleteItemInput
	request.Key = s.keys
	if s.table != "" {
		request.TableName = aws.String(s.table)
	}

	if s.conditionExpression != "" {
		request.ConditionExpression = aws.String(s.conditionExpression)
	}

	if len(s.vals) > 0 {
		request.ExpressionAttributeValues = s.vals
	}
	if len(s.cols) > 0 {
		request.ExpressionAttributeNames = s.cols
	}

	return request, nil
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

	if s.selectedFields != "" {
		query.Select = aws.String("SPECIFIC_ATTRIBUTES")
		query.ProjectionExpression = aws.String(s.selectedFields)
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

	if s.ascending != nil {
		query.ScanIndexForward = s.ascending
	}

	if s.consistent != nil {
		query.ConsistentRead = s.consistent
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

	if s.selectedFields != "" {
		query.Select = aws.String("SPECIFIC_ATTRIBUTES")
		query.ProjectionExpression = aws.String(s.selectedFields)
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

	if s.consistent != nil {
		query.ConsistentRead = s.consistent
	}

	return query, nil
}

// ToGet produces a dynamodb.GetItemInput value based on configured builder
func (s *Builder) ToGet() (dynamodb.GetItemInput, error) {
	if s.err != nil {
		return dynamodb.GetItemInput{}, s.err
	}

	var query dynamodb.GetItemInput

	if len(s.keys) > 0 {
		query.Key = s.keys
	}

	if s.table != "" {
		query.TableName = aws.String(s.table)
	}

	if s.consistent != nil {
		query.ConsistentRead = s.consistent
	}

	return query, nil
}

// ToUpdate produces a dynamodb.UpdateItemInput value based on configured builder
func (s *Builder) ToUpdate() (dynamodb.UpdateItemInput, error) {
	if s.err != nil {
		return dynamodb.UpdateItemInput{}, s.err
	}

	var query dynamodb.UpdateItemInput
	if len(s.keys) > 0 {
		query.Key = s.keys
	}

	if s.updateExpression != "" {
		query.UpdateExpression = aws.String(s.updateExpression)
	}

	if s.conditionExpression != "" {
		query.ConditionExpression = aws.String(s.conditionExpression)
	}

	if len(s.cols) > 0 {
		query.ExpressionAttributeNames = s.cols
	}

	if len(s.vals) > 0 {
		query.ExpressionAttributeValues = s.vals
	}

	if s.table != "" {
		query.TableName = aws.String(s.table)
	}

	return query, nil
}

// ToPut produces a dynamodb.PutItemInput value based on configured builder
func (s *Builder) ToPut(item interface{}) (dynamodb.PutItemInput, error) {
	if s.err != nil {
		return dynamodb.PutItemInput{}, s.err
	}

	var query dynamodb.PutItemInput
	if s.conditionExpression != "" {
		query.ConditionExpression = aws.String(s.conditionExpression)
	}

	if len(s.cols) > 0 {
		query.ExpressionAttributeNames = s.cols
	}

	if len(s.vals) > 0 {
		query.ExpressionAttributeValues = s.vals
	}

	if s.table != "" {
		query.TableName = aws.String(s.table)
	}

	var err error
	query.Item, err = dynamodbattribute.MarshalMap(item)

	return query, err
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
				return "", ErrQueryMisMatch
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
	case int64:
		return typeToAttributeVal(int(v))
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

	return nil, ErrUnsupportedType
}

func sliceToValues(slice interface{}) []interface{} {
	arr := reflect.ValueOf(slice)
	if arr.Kind() != reflect.Slice {
		return nil
	}

	length := arr.Len()
	result := make([]interface{}, length)
	for i := 0; i < length; i++ {
		result[i] = arr.Index(i).Interface()
	}

	return result
}
