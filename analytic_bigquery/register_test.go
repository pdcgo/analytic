package analytic_bigquery_test

import (
	"context"
	"testing"

	"github.com/pdcgo/analytic/analytic_bigquery"
	table "github.com/pdcgo/analytic/analytic_bigquery/table"
	"github.com/stretchr/testify/assert"
)

func TestRegisterTableTest(t *testing.T) {
	ctx := context.Background()
	client, _ := analytic_bigquery.NewClient(ctx)

	t.Run("test real register table", func(t *testing.T) {
		t.Skip()
		err := analytic_bigquery.RegisterAnalyticTable(ctx, table.WithClient(client))
		assert.Nil(t, err)

		err = analytic_bigquery.RegisterEventTable(ctx, table.WithDataset("event"), table.WithClient(client))
		assert.Nil(t, err)
	})

	t.Run("test register analytic table", func(t *testing.T) {
		opts := []table.Option{
			table.WithClient(client),
			table.WithDataset("analytic_test"),
		}
		err := analytic_bigquery.RegisterAnalyticTable(ctx, opts...)
		assert.Nil(t, err)
	})

	t.Run("test register event table", func(t *testing.T) {
		opts := []table.Option{
			table.WithClient(client),
			table.WithDataset("event_test"),
		}
		err := analytic_bigquery.RegisterEventTable(ctx, opts...)
		assert.Nil(t, err)
	})
}
