[![Go Report Card](https://goreportcard.com/badge/github.com/darwayne/dyc)](https://goreportcard.com/report/github.com/darwayne/dyc)
[![GoDoc](https://godoc.org/github.com/darwayne/dyc?status.svg)](https://godoc.org/github.com/darwayne/dyc)
![license](https://img.shields.io/github/license/darwayne/dyc)
# dyc

![image](https://user-images.githubusercontent.com/2807589/92316080-1184e180-efbd-11ea-96a4-df774348ad67.png)


## Examples

### Query
```go
// setup aws session
awsSession := session.Must(session.NewSession(aws.NewConfig()))
// setup aws dynamodb sdk client
db := dynamodb.New(awsSession)
// setup the dyc client
cli := dyc.NewClient(db)

err := cli.Builder().Table("MyTable").
  WhereKey(`PK = ?`, "PartitionKey").
  Index("SomeIndex").
  Where(`'Some'.'Nested'.'Field' = ? AND 'another' = ?`, "cool", true).
  QueryIterate(context.TODO(), func(output *dynamodb.QueryOutput) error {
    // get results
    return nil
  })
```

### Scan
```go
// setup aws session
awsSession := session.Must(session.NewSession(aws.NewConfig()))
// setup aws dynamodb sdk client
db := dynamodb.New(awsSession)
// setup the dyc client
cli := dyc.NewClient(db)

err := cli.Builder().Table("MyTable").
  Where(`'Some'.'Nested'.'Field' = ? AND 'another' = ?`, "cool", true).
  ScanIterate(context.TODO(), func(output *dynamodb.ScanOutput) error {
    // get results
    return nil
  })
```


### ScanDelete
```go
// setup aws session
awsSession := session.Must(session.NewSession(aws.NewConfig()))
// setup aws dynamodb sdk client
db := dynamodb.New(awsSession)
// setup the dyc client
cli := dyc.NewClient(db)

err := cli.Builder().Table("MyTable").
  Where(`'Some'.'Nested'.'Field' = ? AND 'another' = ?`, "cool", true).
  ScanDelete(context.TODO(), dyc.FieldsExtractor("PK", "SK"))
```
 - deletes all records matching the scan
 - PK and SK are the partition key and sort key needed to delete the matching records

### QueryDelete
```go
// setup aws session
awsSession := session.Must(session.NewSession(aws.NewConfig()))
// setup aws dynamodb sdk client
db := dynamodb.New(awsSession)
// setup the dyc client
cli := dyc.NewClient(db)

err := cli.Builder().Table("MyTable").
  WhereKey("PK = ? AND SK BETWEEN ? AND ?", "key", 1, 1337)
  Where(`'Some'.'Nested'.'Field' = ? AND 'another' = ?`, "cool", true).
  QueryDelete(context.TODO(), dyc.FieldsExtractor("ID"))
```
 - deletes all records matching the query
 - ID is the partition key needed to delete the matching records
