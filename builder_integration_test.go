//go:build integration
// +build integration

package dyc

import (
	"context"
	"fmt"
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
			builder = builder.Builder()
			_, err = builder.WhereKey(
				"PK = ?", expected.PK).
				Result(&result).
				QuerySingle(defaultCtx())

			require.NoError(t, err)
			require.Equal(t, expected, result)
			require.Empty(t, builder.PageToken())
		})
	})

	t.Run("ScanDelete", func(t *testing.T) {
		t.Run("happy path", func(t *testing.T) {
			builder := setupBuilder(t)
			const totalRows = 10
			expecations := make([]Row, totalRows)
			for i := 0; i < totalRows; i++ {
				row := genericRow()
				row.SK += fmt.Sprintf("%d", i)
				expecations[i] = row

				_, err := builder.PutItem(defaultCtx(), expecations[i])
				require.NoError(t, err)
			}

			results, err := builder.Builder().Where("PK = ?", expecations[0].PK).ScanAll(defaultCtx())
			require.NoError(t, err)
			require.NotEmpty(t, results)

			err = builder.Builder().Where("PK = ?", expecations[0].PK).ScanDelete(defaultCtx())
			require.NoError(t, err)

			results, err = builder.Builder().Where("PK = ?", expecations[0].PK).ScanAll(defaultCtx())
			require.NoError(t, err)
			require.Empty(t, results)
		})
	})

	t.Run("QueryDelete", func(t *testing.T) {
		t.Run("happy path", func(t *testing.T) {
			builder := setupBuilder(t)
			const totalRows = 10
			expecations := make([]Row, totalRows)
			for i := 0; i < totalRows; i++ {
				row := genericRow()
				row.SK += fmt.Sprintf("%d", i)
				expecations[i] = row

				_, err := builder.PutItem(defaultCtx(), expecations[i])
				require.NoError(t, err)
			}

			results, err := builder.Builder().WhereKey("PK = ?", expecations[0].PK).QueryAll(defaultCtx())
			require.NoError(t, err)
			require.NotEmpty(t, results)

			err = builder.Builder().WhereKey("PK = ?", expecations[0].PK).QueryDelete(defaultCtx())
			require.NoError(t, err)

			results, err = builder.Builder().WhereKey("PK = ?", expecations[0].PK).QueryAll(defaultCtx())
			require.NoError(t, err)
			require.Empty(t, results)
		})
	})

	t.Run("QueryAll", func(t *testing.T) {
		t.Run("happy path", func(t *testing.T) {
			builder := setupBuilder(t)
			expected := genericRow()
			_, err := builder.PutItem(defaultCtx(), expected)
			require.NoError(t, err)

			var result []Row
			builder = builder.Builder()
			_, err = builder.WhereKey(
				"PK = ?", expected.PK).
				Result(&result).
				QueryAll(defaultCtx())

			require.NoError(t, err)
			require.NotEmpty(t, result)
			require.Equal(t, expected, result[0])
			require.Empty(t, builder.PageToken())
		})

		t.Run("cursor and limit should behave as expected", func(t *testing.T) {
			builder := setupBuilder(t)
			const totalRows = 10
			expecations := make([]Row, totalRows)
			for i := 0; i < totalRows; i++ {
				row := genericRow()
				row.SK += fmt.Sprintf("%d", i)
				expecations[i] = row

				_, err := builder.PutItem(defaultCtx(), expecations[i])
				require.NoError(t, err)
			}

			t.Run("should return expected rows when no limit or cursor set", func(t *testing.T) {
				var result []Row
				b := builder.Builder()
				_, err := b.WhereKey(
					"PK = ?", expecations[0].PK).
					Result(&result).
					QueryAll(defaultCtx())

				require.Len(t, result, totalRows)

				require.NoError(t, err)
				require.NotEmpty(t, result)
				for i := 0; i < totalRows; i++ {
					require.Equal(t, expecations[i], result[i])
				}

				require.Empty(t, b.PageToken())
			})

			t.Run("should paginate as expected", func(t *testing.T) {
				var result []Row
				b := builder.Builder()
				_, err := b.WhereKey(
					"PK = ?", expecations[0].PK).
					Result(&result).
					Limit(5).
					QueryAll(defaultCtx())

				require.Len(t, result, 5)

				require.NoError(t, err)
				require.NotEmpty(t, result)
				for i := 0; i < 5; i++ {
					require.Equal(t, expecations[i], result[i])
				}

				require.NotEmpty(t, b.PageToken())

				t.Run("cursor should return expected results", func(t *testing.T) {
					var result2 []Row
					c := builder.Builder()
					_, err := c.WhereKey(
						"PK = ?", expecations[0].PK).
						Result(&result2).
						Cursor(b.PageToken()).
						Limit(5).
						QueryAll(defaultCtx())

					require.Len(t, result2, 5)

					require.NoError(t, err)
					require.NotEmpty(t, result2)
					x := 0
					for i := 5; i < 10; i++ {
						require.Equal(t, expecations[i], result2[x])
						x++
					}

					require.Empty(t, c.PageToken())
				})
			})
		})
	})

	t.Run("ScanAll", func(t *testing.T) {
		t.Run("happy path", func(t *testing.T) {
			builder := setupBuilder(t)
			expected := genericRow()
			_, err := builder.PutItem(defaultCtx(), expected)
			require.NoError(t, err)

			var result []Row
			builder = builder.Builder()
			_, err = builder.Where(
				"PK = ?", expected.PK).
				Result(&result).
				ScanAll(defaultCtx())

			require.NoError(t, err)
			require.NotEmpty(t, result)
			require.Equal(t, expected, result[0])
			require.Empty(t, builder.PageToken())
		})

		t.Run("cursor and limit should behave as expected", func(t *testing.T) {
			builder := setupBuilder(t)
			const totalRows = 10
			expecations := make([]Row, totalRows)
			for i := 0; i < totalRows; i++ {
				row := genericRow()
				row.SK += fmt.Sprintf("%d", i)
				expecations[i] = row

				_, err := builder.PutItem(defaultCtx(), expecations[i])
				require.NoError(t, err)
			}

			t.Run("should return expected rows when no limit or cursor set", func(t *testing.T) {
				var result []Row
				b := builder.Builder()
				_, err := b.Where(
					"PK = ?", expecations[0].PK).
					Result(&result).
					ScanAll(defaultCtx())

				require.Len(t, result, totalRows)

				require.NoError(t, err)
				require.NotEmpty(t, result)
				for i := 0; i < totalRows; i++ {
					require.Equal(t, expecations[i], result[i])
				}

				require.Empty(t, b.PageToken())
			})

			t.Run("should paginate as expected", func(t *testing.T) {
				var result []Row
				b := builder.Builder()
				_, err := b.Where(
					"PK = ?", expecations[0].PK).
					Result(&result).
					Limit(5).
					ScanAll(defaultCtx())

				require.Len(t, result, 5)

				require.NoError(t, err)
				require.NotEmpty(t, result)
				for i := 0; i < 5; i++ {
					require.Equal(t, expecations[i], result[i])
				}

				require.NotEmpty(t, b.PageToken())

				t.Run("cursor should return expected results", func(t *testing.T) {
					var result2 []Row
					c := builder.Builder()
					_, err := c.Where(
						"PK = ?", expecations[0].PK).
						Result(&result2).
						Cursor(b.PageToken()).
						Limit(5).
						ScanAll(defaultCtx())

					require.Len(t, result2, 5)

					require.NoError(t, err)
					require.NotEmpty(t, result2)
					x := 0
					for i := 5; i < 10; i++ {
						require.Equal(t, expecations[i], result2[x])
						x++
					}

					require.Empty(t, c.PageToken())
				})
			})
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
