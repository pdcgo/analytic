package analytic_bigquery_test

import (
	"context"
	"fmt"
	"log"
	"testing"

	"github.com/pdcgo/analytic/analytic"
	"github.com/pdcgo/analytic/analytic_bigquery"
	table "github.com/pdcgo/analytic/analytic_bigquery/table"
	"github.com/pdcgo/common_conf/pdc_common"
	"github.com/stretchr/testify/assert"
)

func getProducts() []analytic.BannedProduct {
	products := []analytic.BannedProduct{}
	for i := 1; i <= 3; i++ {
		product := analytic.BannedProduct{
			ShopId: int64(i),
			ItemId: int64(i),
			Title:  fmt.Sprintf("Product %d", i),
			Status: "sample",
		}
		product.SetHash()
		log.Println("hash", product.Hash)
		products = append(products, product)
	}
	return products
}

func TestBannedOrderSync(t *testing.T) {
	AnalyticDB.CleanUp()
	ctx := context.Background()

	config := pdc_common.GetConfig()
	client, _ := analytic_bigquery.NewClient(config.Credential, ctx)
	repo := analytic.NewShopeeBannedProductRepo(AnalyticDB)

	// register table
	opts := []table.Option{
		table.WithClient(client),
		table.WithDataset("analytic_test"),
	}
	err := analytic_bigquery.RegisterAnalyticTable(ctx, opts...)
	assert.Nil(t, err)

	// store orders
	products := getProducts()
	err = repo.StoreProducts(products)
	assert.Nil(t, err)

	// store bigquery
	orderSync := analytic_bigquery.NewBannedProductSync(repo, opts...)
	err = orderSync.Sync(ctx)
	assert.Nil(t, err)
}
