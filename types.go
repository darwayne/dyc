package dyc

import (
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// StringSet converts an array of strings to a string set type
func StringSet(arr ...string) *dynamodb.AttributeValue {
	return &dynamodb.AttributeValue{
		SS: aws.StringSlice(arr),
	}
}

// StringList converts an array of strings to a string list type
func StringList(arr ...string) *dynamodb.AttributeValue {
	list := make([]*dynamodb.AttributeValue, 0, len(arr))
	for _, str := range arr {
		list = append(list, String(str))
	}
	return &dynamodb.AttributeValue{
		L: list,
	}
}

// IntList converts an array of integers to an integer list type
func IntList(arr ...int) *dynamodb.AttributeValue {
	list := make([]*dynamodb.AttributeValue, 0, len(arr))
	for _, num := range arr {
		list = append(list, Int(num))
	}
	return &dynamodb.AttributeValue{
		L: list,
	}
}

// String converts a string to a string type
func String(str string) *dynamodb.AttributeValue {
	return &dynamodb.AttributeValue{
		S: &str,
	}
}

// Int converts an integer into a integer type
func Int(num int) *dynamodb.AttributeValue {
	return &dynamodb.AttributeValue{
		N: aws.String(strconv.FormatInt(int64(num), 10)),
	}
}
