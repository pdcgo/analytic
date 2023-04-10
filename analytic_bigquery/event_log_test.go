package analytic_bigquery_test

import (
	"context"
	"errors"
	"testing"

	"github.com/pdcgo/analytic/analytic_bigquery"
	table "github.com/pdcgo/analytic/analytic_bigquery/table"
)

func TestEventLog(t *testing.T) {
	ctx := context.Background()
	client, _ := analytic_bigquery.NewClient(ctx)

	opts := []table.Option{
		table.WithClient(client),
		table.WithDataset("event_test"),
	}
	event := analytic_bigquery.NewEventLog("test", "test", opts...)
	err := errors.New("test error")
	defer event.StoreLogs(ctx)

	event.AddStartLog()
	event.AddErrorLog(err)
	event.AddFinishLog()
}
