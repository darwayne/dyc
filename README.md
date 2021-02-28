[![Go Report Card](https://goreportcard.com/badge/github.com/darwayne/dyc)](https://goreportcard.com/report/github.com/darwayne/dyc)
[![GoDoc](https://godoc.org/github.com/darwayne/dyc?status.svg)](https://godoc.org/github.com/darwayne/dyc)
![license](https://img.shields.io/github/license/darwayne/dyc)
# dyc

<p align="center"><img src="https://user-images.githubusercontent.com/2807589/92316080-1184e180-efbd-11ea-96a4-df774348ad67.png" width="300"></p>

`dyc` is a golang package which provides a dynamodb client and query builder. It utilizes the go AWS SDK to provide a more convenient way to interact with dynamodb.

### Features
 - Utilize current dynamodb query language
 - Easily reference columns that conflict with dynamodb reserved words by wrapping column in a single quote
 - Build queries with ease
 - Input substitution via `?`
 - Parallel Scan support
 - Copy table support
 - In Support
 - Basic Conjunctions support

### Examples

#### Client setup
```go
// setup aws session
awsSession := session.Must(session.NewSession(aws.NewConfig()))
// setup aws dynamodb sdk client
db := dynamodb.New(awsSession)
// setup the dyc client
cli := dyc.NewClient(db)
```

#### Query
***Iterator***
```go
// error ignored for demonstration purposes only
it, err := li.Builder().Table("MyTable").
             WhereKey(`PK = ?`, "PartitionKey").
             Index("SomeIndex").
             // Where is equivalent to filter expression
             Where(`'Some'.'Nested'.'Field' = ? AND 'another' = ?`, "cool", true).
             ToQueryIterator(ctx)

var results []map[string]dynamodb.AttributeValue
for it.Next() {
  // error ignored for demonstration purposes only
  output, _ := it.QueryValue(), it.Err()
  results = append(results, output.Items...)
}
```
***Iterate***
```go
err := cli.Builder().Table("MyTable").
  WhereKey(`PK = ?`, "PartitionKey").
  Index("SomeIndex").
  // Where is equivalent to filter expression
  Where(`'Some'.'Nested'.'Field' = ? AND 'another' = ?`, "cool", true).
  QueryIterate(ctx, func(output *dynamodb.QueryOutput) error {
    // get results
    return nil
  })
```

***Query All***
```go
type Row struct {
	PK     string
	SK     string
	Some map[string]interface
}

var rows []Row
// results will be an array of dynamodb attribute maps
results, err := cli.Builder().Table("MyTable").
  // WhereKey is equivalent to query expression
  WhereKey(`PK = ?`, "PartitionKey").
  // Result Unmarshals the raw results into the custom type
  Result(&rows).
  Index("SomeIndex").
  Where(`'Some'.'Nested'.'Field' = ? AND 'another' = ?`, "cool", true).
  QueryAll(ctx context.Context)
```

***Query Single***
```go
type Row struct {
	PK     string
	SK     string
	Some map[string]interface
}

var row Row
//result will be a dynamodb attribute map
result, err := cli.Builder().Table("MyTable").
  WhereKey(`PK = ?`, "PartitionKey").
  // Result Unmarshals the raw results into the custom type
  Result(&row).
  Index("SomeIndex").
  Where(`'Some'.'Nested'.'Field' = ? AND 'another' = ?`, "cool", true).
  QuerySingle(ctx context.Context)
```

***Delete By Query***
```go
primaryKeyExtractor := dyc.FieldsExtractor("PK", "SK")
err := cli.Builder().Table("MyTable").
  WhereKey("PK = ? AND SK BETWEEN ? AND ?", "key", 1, 1337)
  Where(`'Some'.'Nested'.'Field' = ? AND 'another' = ?`, "cool", true).
  QueryDelete(ctx, primaryKeyExtractor)
```
 - deletes all records matching the query
 
***Insert Item***
```go
type Row struct {
	PK     string
	SK     string
	StrMap map[string]string
}
row := Row{PK: "yo", SK: "lo", StrMap: map[string]string{"who": "goes there"}}
result, err := cli.Builder().Table("MyTable").
  PutItem(context.Background(), row)

```

***Insert Item Conditionally***
```go
type Row struct {
	PK     string
	SK     string
	StrMap map[string]string
}
row := Row{PK: "yo", SK: "lo", StrMap: map[string]string{"who": "goes there"}}
result, err := cli.Builder().Table("MyTable").
  Condition("attribute_not_exists(PK)").
  PutItem(context.Background(), row)

```

***Update Item***
```go
result, err := cli.Builder().Table("MyTable").
  Key("PK", "yo", "SK", "lo").
  Update(`REMOVE 'StrMap'.'who' SET 'StrMap'.'goes' = ?`, "who there").
  UpdateItem(context.Background())
```

#### Scan
***Iterator***
```go
// error ignored for demonstration purposes only
it, err := li.Builder().Table("MyTable").
             WhereKey(`PK = ?`, "PartitionKey").
             Index("SomeIndex").
             // Where is equivalent to filter expression
             Where(`'Some'.'Nested'.'Field' = ? AND 'another' = ?`, "cool", true).
             ToScanIterator(ctx)

var results []map[string]dynamodb.AttributeValue
for it.Next() {
  // error ignored for demonstration purposes only
  output, _ := it.ScanValue(), it.Err()
  results = append(results, output.Items...)
}
```

***Iterate***
```go
err := cli.Builder().Table("MyTable").
  Where(`'Some'.'Nested'.'Field' = ? AND 'another' = ?`, "cool", true).
  ScanIterate(ctx, func(output *dynamodb.ScanOutput) error {
    // get results
    return nil
  })
```

***In***
```go
err := cli.Builder().Table("MyTable").
  IN(`'Some'.'Nested'.'Field'`, "value1", "value2").
  ScanIterate(ctx, func(output *dynamodb.ScanOutput) error {
    // get results
    return nil
  })
```

***InSlice***
```go
err := cli.Builder().Table("MyTable").
  IN(`'Some'.'Nested'.'Field'`, []string{"value1", "value2"}).
  ScanIterate(ctx, func(output *dynamodb.ScanOutput) error {
    // get results
    return nil
  })
```

***Conjunctions Example***
```go
results, err := cli.Builder().
  Table("MyTable").
  Select(`PK`, "DAT", "SOME.NESTED.FIELD").
  WhereKey("PK = ?", "some key").
  Where(`'SOMEVALUE'.here = ?`, true).
  OrWhere(`'SOMEOTHER'.'VALUE' = ?`, false).
  OrIN("field1", 23, 432, 200, 23).
  InSlice("field2", []string{"one", "two", "three"})
  Condition("attribute_not_exists(YO.'id')").
  OrCondition("attribute_exists(YOLO)").
  IN(`'Some'.'Nested'.'Field'`, []string{"value1", "value2"}).
  QueryAll(context.TODO())
```

***Scan Delete***
```go
err := cli.Builder().Table("MyTable").
  Where(`'Some'.'Nested'.'Field' = ? AND 'another' = ?`, "cool", true).
  ScanDelete(ctx, dyc.FieldsExtractor("PK", "SK"))
```
 - deletes all records matching the scan
 - PK and SK are the partition key and sort key needed to delete the matching records


#### Copy table example
```go
totalWorkers := 40
err := cli.CopyTable(ctx, "destinationTable", "sourceTable", totalWorkers, nil) 
```
