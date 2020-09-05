package dyc

import "github.com/aws/aws-sdk-go/service/dynamodb"

type KeyExtractor func(map[string]*dynamodb.AttributeValue) map[string]*dynamodb.AttributeValue

func PKSKExtractor(attrs map[string]*dynamodb.AttributeValue) map[string]*dynamodb.AttributeValue {
	return extractFields(attrs, "PK", "SK")
}

func extractFields(data map[string]*dynamodb.AttributeValue, fields ...string) map[string]*dynamodb.AttributeValue {
	result := make(map[string]*dynamodb.AttributeValue, len(fields))

	for _, field := range fields {
		val, found := data[field]
		if !found {
			continue
		}
		result[field] = val
	}

	return result
}
