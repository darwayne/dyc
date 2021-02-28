package dynamotest

import (
	"sort"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

var DefaultThroughput = &dynamodb.ProvisionedThroughput{
	ReadCapacityUnits:  aws.Int64(25),
	WriteCapacityUnits: aws.Int64(25),
}

func DefaultKeySchema() []*dynamodb.KeySchemaElement {
	return toDynamoKeySchema(map[string]string{
		"PK": "HASH",
		"SK": "RANGE",
	})
}

func DefaultDefinitions() []*dynamodb.AttributeDefinition {
	return toDynamoAttrs(map[string]string{
		"PK":      "S",
		"SK":      "S",
		"TYP":     "S",
		"GSI1PK":  "S",
		"GSI1SK":  "S",
		"GSI2PK":  "S",
		"GSI2SK":  "S",
		"GSI3PK":  "S",
		"GSI3SK":  "S",
		"GSI1PKS": "S",
		"GSI1SKN": "N",
		"GSI2PKS": "S",
		"GSI2SKN": "N",
		"GSI3PKS": "S",
		"GSI3SKN": "N",
	})
}

func DefaultGSIConfiguration() []*dynamodb.GlobalSecondaryIndex {
	return []*dynamodb.GlobalSecondaryIndex{
		{
			IndexName: aws.String("TYPE"),
			KeySchema: toDynamoKeySchema(map[string]string{
				"TYP": "HASH",
			}),
			Projection: &dynamodb.Projection{
				ProjectionType: aws.String("ALL"),
			},
			ProvisionedThroughput: DefaultThroughput,
		},
		{
			IndexName: aws.String("GSI1"),
			KeySchema: toDynamoKeySchema(map[string]string{
				"GSI1PK": "HASH",
				"GSI1SK": "RANGE",
			}),
			Projection: &dynamodb.Projection{
				ProjectionType: aws.String("ALL"),
			},
			ProvisionedThroughput: DefaultThroughput,
		},
		{
			IndexName: aws.String("GSI2"),
			KeySchema: toDynamoKeySchema(map[string]string{
				"GSI2PK": "HASH",
				"GSI2SK": "RANGE",
			}),
			Projection: &dynamodb.Projection{
				ProjectionType: aws.String("ALL"),
			},
			ProvisionedThroughput: DefaultThroughput,
		},
		{
			IndexName: aws.String("GSI3"),
			KeySchema: toDynamoKeySchema(map[string]string{
				"GSI3PK": "HASH",
				"GSI3SK": "RANGE",
			}),
			Projection: &dynamodb.Projection{
				ProjectionType: aws.String("ALL"),
			},
			ProvisionedThroughput: DefaultThroughput,
		},
		// == number based GSIs with hash as string and sort key as number
		{
			IndexName: aws.String("GSI1SKN"),
			KeySchema: toDynamoKeySchema(map[string]string{
				"GSI1PKS": "HASH",
				"GSI1SKN": "RANGE",
			}),
			Projection: &dynamodb.Projection{
				ProjectionType: aws.String("ALL"),
			},
			ProvisionedThroughput: DefaultThroughput,
		},
		{
			IndexName: aws.String("GSI2SKN"),
			KeySchema: toDynamoKeySchema(map[string]string{
				"GSI2PKS": "HASH",
				"GSI2SKN": "RANGE",
			}),
			Projection: &dynamodb.Projection{
				ProjectionType: aws.String("ALL"),
			},
			ProvisionedThroughput: DefaultThroughput,
		},
		{
			IndexName: aws.String("GSI3SKN"),
			KeySchema: toDynamoKeySchema(map[string]string{
				"GSI3PKS": "HASH",
				"GSI3SKN": "RANGE",
			}),
			Projection: &dynamodb.Projection{
				ProjectionType: aws.String("ALL"),
			},
			ProvisionedThroughput: DefaultThroughput,
		},
	}
}

func DefaultSchema() Schema {
	return Schema{
		KeySchema: DefaultKeySchema(),
		Attrs:     DefaultDefinitions(),
		GSI:       DefaultGSIConfiguration(),
	}
}

type Schema struct {
	KeySchema []*dynamodb.KeySchemaElement
	Attrs     []*dynamodb.AttributeDefinition
	GSI       []*dynamodb.GlobalSecondaryIndex
}

func toDynamoKeySchema(keySchema map[string]string) []*dynamodb.KeySchemaElement {
	result := make([]*dynamodb.KeySchemaElement, 0, len(keySchema))
	for k, v := range keySchema {
		result = append(result, &dynamodb.KeySchemaElement{
			AttributeName: aws.String(k),
			KeyType:       aws.String(v),
		})
	}

	sort.Slice(result, func(i, j int) bool {
		return *result[i].KeyType < *result[j].KeyType
	})

	return result
}

func toDynamoAttrs(attrs map[string]string) []*dynamodb.AttributeDefinition {
	result := make([]*dynamodb.AttributeDefinition, 0, len(attrs))
	for k, v := range attrs {
		result = append(result, &dynamodb.AttributeDefinition{
			AttributeName: aws.String(k),
			AttributeType: aws.String(v),
		})
	}

	sort.Slice(result, func(i, j int) bool {
		return *result[i].AttributeName < *result[j].AttributeName
	})

	return result
}
