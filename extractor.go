package dyc

import "github.com/aws/aws-sdk-go/service/dynamodb"

// KeyExtractor is a type primarily used to get necessary fields needed to delete a record
type KeyExtractor func(map[string]*dynamodb.AttributeValue) map[string]*dynamodb.AttributeValue

// PKSKExtractor extracts the fields PK and SK
func PKSKExtractor(attrs map[string]*dynamodb.AttributeValue) map[string]*dynamodb.AttributeValue {
	return extractFields(attrs, "PK", "SK")
}

// FieldsExtractor extracts the provided fields
func FieldsExtractor(fields ...string) KeyExtractor {
	return func(m map[string]*dynamodb.AttributeValue) map[string]*dynamodb.AttributeValue {
		return extractFields(m, fields...)
	}
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
