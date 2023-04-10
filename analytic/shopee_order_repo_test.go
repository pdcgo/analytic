package analytic_test

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"testing"

	"github.com/pdcgo/analytic/analytic"
	"github.com/stretchr/testify/assert"
)

func getOrders() []analytic.Order {
	var hasil []analytic.Order

	base, _ := os.Getwd()

	data, _ := os.ReadFile(filepath.Join(base, "../test/assets/analytic.json"))

	json.Unmarshal(data, &hasil)
	return hasil
}

func TestShopeeOrderRepo(t *testing.T) {
	AnalyticDB.CleanUp()
	repo := analytic.NewShopeeOrderRepo(AnalyticDB)

	t.Run("test shopee order repo store", func(t *testing.T) {
		// store orders
		orders := getOrders()
		err := repo.StoreOrders(orders)
		assert.Nil(t, err)

		// check uploaded
		orders = repo.RemoveDuplicates(orders)
		assert.Equal(t, 0, len(orders))
	})

	t.Run("test shopee order repo get unsync", func(t *testing.T) {
		// store orders
		orders, _ := repo.GetUnsyncOrders()
		for _, order := range orders {
			log.Printf("Unsync Orderid: %d", order.Id)
			assert.False(t, order.Sync)
		}
	})

	t.Run("test shopee order repo sync", func(t *testing.T) {
		AnalyticDB.CleanUp()
		orders := getOrders()
		err := repo.StoreOrders(orders)
		assert.Nil(t, err)

		// sync orders
		err = repo.SyncOrders()
		assert.Nil(t, err)
	})
}
