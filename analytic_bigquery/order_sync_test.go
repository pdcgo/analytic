package analytic_bigquery_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/pdcgo/analytic/analytic"
	"github.com/pdcgo/analytic/analytic_bigquery"
	table "github.com/pdcgo/analytic/analytic_bigquery/table"
	"github.com/pdcgo/common_conf/pdc_common"
	"github.com/stretchr/testify/assert"
)

func getOrders() []analytic.Order {
	var hasil []analytic.Order

	base, _ := os.Getwd()

	data, _ := os.ReadFile(filepath.Join(base, "../test/assets/analytic.json"))

	json.Unmarshal(data, &hasil)
	return hasil
}

func TestOrderSync(t *testing.T) {
	AnalyticDB.CleanUp()
	ctx := context.Background()
	config := pdc_common.GetConfig()
	client, _ := analytic_bigquery.NewClient(config.Credential, ctx)
	repo := analytic.NewShopeeOrderRepo(AnalyticDB)

	// register table
	opts := []table.Option{
		table.WithClient(client),
		table.WithDataset("analytic_test"),
	}
	err := analytic_bigquery.RegisterAnalyticTable(ctx, opts...)
	assert.Nil(t, err)

	// store orders
	orders := getOrders()
	err = repo.StoreOrders(orders)
	assert.Nil(t, err)

	// store bigquery
	orderSync := analytic_bigquery.NewBigqueryOrderSync(repo, opts...)
	err = orderSync.Sync(ctx, 0)
	assert.Nil(t, err)
}
