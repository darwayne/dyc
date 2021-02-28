package dynamotest

import (
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/stretchr/testify/require"
)

func endpoint() string {
	// env variable to run the test
	endpoint := os.Getenv("DYNAMO_ENDPOINT")

	if endpoint == "" {
		endpoint = "http://127.0.0.1:47801"
	}

	return endpoint
}

func SetupTestTable(t *testing.T, parentCtx context.Context, tableName string, schema Schema) (string, *dynamodb.DynamoDB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parentCtx, 30*time.Second)
	defer cancel()

	endpoint := endpoint()
	db := setupDynamoSession(t, endpoint)
	table := setupDynamoTable(t, ctx, db, tableName, schema)

	return table, db
}

func setupDynamoSession(t *testing.T, endpoint string) *dynamodb.DynamoDB {
	t.Helper()
	sess, err := session.NewSession(&aws.Config{
		CredentialsChainVerboseErrors: aws.Bool(true),
		Credentials:                   credentials.NewStaticCredentials("NOT_REAL", "FAKE", "123123213"),
		Region:                        aws.String("us-east-1"),
		Endpoint:                      aws.String(endpoint),
	})

	require.NoError(t, err, "error setting up aws session for test")

	return dynamodb.New(sess)
}

func setupDynamoTable(t *testing.T, ctx context.Context, db dynamodbiface.DynamoDBAPI, tableName string, schema Schema) string {
	t.Helper()

	hash := md5.Sum(randomBytes(t, 32))
	id := hex.EncodeToString(hash[:])

	table := fmt.Sprintf("%s-%s", tableName, id)

	err := createDynamoTable(ctx, db, table, schema)

	require.NoError(t, err, "error while creating table")

	return table
}

// Sets up a dynamo table with the provided attribute
func createDynamoTable(ctx context.Context, db dynamodbiface.DynamoDBAPI, table string, schema Schema) error {
	_, err := db.CreateTable(&dynamodb.CreateTableInput{
		TableName:              &table,
		AttributeDefinitions:   schema.Attrs,
		KeySchema:              schema.KeySchema,
		ProvisionedThroughput:  DefaultThroughput,
		GlobalSecondaryIndexes: schema.GSI,
	})

	return err
}

func randomBytes(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	_, err := rand.Read(b)
	require.NoError(t, err, "error reading random bytes")

	return b
}
