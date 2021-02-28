//+build integration

package dyc

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/darwayne/dyc/internal/testing/dynamotest"
)

type Row struct {
	PK     string
	SK     string
	StrMap map[string]string
}

func TestBuilder(t *testing.T) {
	t.Run("PutItem", func(t *testing.T) {
		t.Run("happy path", func(t *testing.T) {
			builder := setupBuilder(t)
			_, err := builder.PutItem(defaultCtx(), genericRow())
			require.NoError(t, err)
		})

		t.Run("condition should work", func(t *testing.T) {
			builder := setupBuilder(t)
			_, err := builder.PutItem(defaultCtx(), genericRow())
			require.NoError(t, err)

			_, err = builder.Builder().Condition("attribute_not_exists(PK)").
				PutItem(defaultCtx(), genericRow())
			require.Error(t, err)

		})
	})

	t.Run("GetItem", func(t *testing.T) {
		t.Run("happy path", func(t *testing.T) {
			builder := setupBuilder(t)
			expected := genericRow()
			_, err := builder.PutItem(defaultCtx(), expected)
			require.NoError(t, err)

			var result Row
			_, err = builder.Builder().Key("PK", expected.PK, "SK", expected.SK).
				Result(&result).
				GetItem(defaultCtx())

			require.NoError(t, err)
			require.Equal(t, expected, result)
		})
	})

	t.Run("QuerySingle", func(t *testing.T) {
		t.Run("happy path", func(t *testing.T) {
			builder := setupBuilder(t)
			expected := genericRow()
			_, err := builder.PutItem(defaultCtx(), expected)
			require.NoError(t, err)

			var result Row
			_, err = builder.Builder().WhereKey(
				"PK = ?", expected.PK).
				Result(&result).
				QuerySingle(defaultCtx())

			require.NoError(t, err)
			require.Equal(t, expected, result)
		})
	})

	t.Run("UpdateItem", func(t *testing.T) {
		t.Run("happy path", func(t *testing.T) {
			builder := setupBuilder(t)
			expected := genericRow()
			_, err := builder.PutItem(defaultCtx(), expected)
			require.NoError(t, err)

			t.Run("should REMOVE as expected", func(t *testing.T) {
				_, err = builder.Builder().Key("PK", expected.PK, "SK", expected.SK).
					Update(`REMOVE 'StrMap'.'darwayne'`).
					UpdateItem(defaultCtx())

				require.NoError(t, err)

				var result Row
				_, err = builder.Builder().WhereKey(
					"PK = ?", expected.PK).
					Result(&result).
					QuerySingle(defaultCtx())

				require.NoError(t, err)

				require.Empty(t, result.StrMap)
			})

			t.Run("should SET as expected", func(t *testing.T) {
				_, err = builder.Builder().Key("PK", expected.PK, "SK", expected.SK).
					Update(`SET 'StrMap'.'yolo' = ?`, "once").
					UpdateItem(defaultCtx())

				require.NoError(t, err)

				var result Row
				_, err = builder.Builder().WhereKey(
					"PK = ?", expected.PK).
					ConsistentRead(true).
					Result(&result).
					QuerySingle(defaultCtx())

				require.NoError(t, err)

				require.NotEmpty(t, result.StrMap)
				require.Equal(t, "once", result.StrMap["yolo"])
			})

		})

	})
}

func genericRow() Row {
	return Row{
		PK: "ONE",
		SK: "TWO",
		StrMap: map[string]string{
			"darwayne": "was here",
		},
	}
}

func defaultCtx() context.Context {
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	return ctx
}

func setupBuilder(t *testing.T) *Builder {
	t.Helper()
	t.Parallel()
	table, db := dynamotest.SetupTestTable(context.Background(), t, "builder", dynamotest.DefaultSchema())

	return NewClient(db).Builder().Table(table)
}
