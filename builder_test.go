//+build unit

package dyc

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuilder_Where(t *testing.T) {
	t.Run("happy path", func(t *testing.T) {
		b := NewBuilder()
		b.Where(`DAT.'super'.'nested'.'field' = ?`, 1)

		require.Empty(t, b.err)
		assert.Equal(t, "DAT.#1.#2.#3 = :0", b.filterExpresion)
		require.Len(t, b.cols, 3)

		require.NotEmpty(t, b.cols["#1"])
		require.NotEmpty(t, b.cols["#2"])
		require.NotEmpty(t, b.cols["#3"])

		assert.Equal(t, "super", *b.cols["#1"])
		assert.Equal(t, "nested", *b.cols["#2"])
		assert.Equal(t, "field", *b.cols["#3"])

		require.NotEmpty(t, b.vals)
		require.NotEmpty(t, b.vals[":0"])
		require.NotEmpty(t, "1", b.vals[":0"].N)
	})

	t.Run("with errors", func(t *testing.T) {
		t.Run("should short circuit", func(t *testing.T) {
			b := NewBuilder()
			b.err = errors.New("something")
			b.Where(`DAT.'super'.'nested'.'field' = ?`, 1)

			require.Empty(t, b.filterExpresion)
		})
	})
}

func TestBuilder_WhereKey(t *testing.T) {
	t.Run("happy path", func(t *testing.T) {
		b := NewBuilder()
		b.WhereKey(`DAT.'super'.'nested'.'field' = ? AND blah = ?`, 1, "yo")

		require.Empty(t, b.err)
		assert.Equal(t, "DAT.#1.#2.#3 = :0 AND blah = :1", b.keyExpression)
		require.Len(t, b.cols, 3)

		require.NotEmpty(t, b.cols["#1"])
		require.NotEmpty(t, b.cols["#2"])
		require.NotEmpty(t, b.cols["#3"])

		assert.Equal(t, "super", *b.cols["#1"])
		assert.Equal(t, "nested", *b.cols["#2"])
		assert.Equal(t, "field", *b.cols["#3"])

		require.NotEmpty(t, b.vals)
		require.NotEmpty(t, b.vals[":0"])
		require.NotEmpty(t, b.vals[":1"])
		require.Equal(t, "1", *b.vals[":0"].N)
		require.Equal(t, "yo", *b.vals[":1"].S)
	})

	t.Run("with errors", func(t *testing.T) {
		t.Run("should short circuit", func(t *testing.T) {
			b := NewBuilder()
			b.err = errors.New("something")
			b.WhereKey(`DAT.'super'.'nested'.'field' = ?`, 1)

			require.Empty(t, b.filterExpresion)
		})
	})
}

func TestBuilder_ToQuery(t *testing.T) {
	b := NewBuilder().
		Where(`DAT.'super'.'nested'.'field' = ? AND blah = ?`, 1, "yo").
		WhereKey("'interesting' = ?", true).
		Index("SomeIndex").
		Table("SomeTable")

	t.Run("happy path", func(t *testing.T) {
		result, err := b.ToQuery()
		require.NoError(t, err)

		require.NotEmpty(t, result.TableName)
		require.NotEmpty(t, result.IndexName)
		require.NotEmpty(t, result.FilterExpression)
		require.NotEmpty(t, result.KeyConditionExpression)
		require.NotEmpty(t, result.ExpressionAttributeNames)
		require.NotEmpty(t, result.ExpressionAttributeValues)

		assert.Equal(t, "SomeIndex", *result.IndexName)
		assert.Equal(t, "SomeTable", *result.TableName)
	})

	t.Run("with errors", func(t *testing.T) {
		t.Run("should short circuit", func(t *testing.T) {
			b := NewBuilder()
			b.err = errors.New("something")
			b.WhereKey(`DAT.'super'.'nested'.'field' = ?`, 1)

			_, err := b.ToQuery()
			require.Error(t, err)
		})
	})
}
