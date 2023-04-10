package analytic_test

import (
	"fmt"
	"log"
	"testing"

	"github.com/pdcgo/analytic/analytic"
	"github.com/stretchr/testify/assert"
)

func getBannedProducts() []analytic.BannedProduct {
	products := []analytic.BannedProduct{}
	for i := 1; i <= 3; i++ {
		product := analytic.BannedProduct{
			ShopId: int64(i),
			ItemId: int64(i),
			Title:  fmt.Sprintf("Product %d", i),
			Status: "sample",
		}
		product.SetHash()
		products = append(products, product)
	}
	return products
}

func TestShopeeBannedProductRepo(t *testing.T) {
	AnalyticDB.CleanUp()
	repo := analytic.NewShopeeBannedProductRepo(AnalyticDB)

	t.Run("test banned product repo store", func(t *testing.T) {
		// store products
		products := getBannedProducts()
		err := repo.StoreProducts(products)
		assert.Nil(t, err)

		// check uploaded
		products = repo.RemoveDuplicates(products)
		assert.Equal(t, 0, len(products))
	})

	t.Run("test banned product repo get unsync", func(t *testing.T) {
		products, _ := repo.GetUnsyncProducts()
		for _, product := range products {
			log.Printf("Unsync Product: %d", product.ItemId)
			assert.False(t, product.Sync)
		}
	})

	t.Run("test banned product repo product sync", func(t *testing.T) {
		// store products
		products := getBannedProducts()
		err := repo.StoreProducts(products)
		assert.Nil(t, err)

		// sync products
		err = repo.SyncProducts()
		assert.Nil(t, err)
	})
}
