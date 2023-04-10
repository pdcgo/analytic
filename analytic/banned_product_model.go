package analytic

import (
	"database/sql"
	"time"

	"gorm.io/gorm"
)

const (
	CHECK_PRODUCT_SAMPLE              = "sample"
	CHECK_PRODUCT_DIBLOKIR            = "diblokir"
	CHECK_PRODUCT_DITURUNKAN          = "diturunkan"
	CHECK_PRODUCT_DIHAPUS             = "dihapus"
	CHECK_PRODUCT_RIWAYAT_PELANGGARAN = "riwayat_pelanggaran"
)

type BannedProduct struct {
	gorm.Model
	ItemId     int64 `gorm:"primaryKey"`
	ShopId     int64 `gorm:"primaryKey"`
	CatId      int32
	Hash       string `gorm:"primaryKey"`
	Sync       bool
	Title      string
	Image      string
	Status     string
	Penalty    sql.NullString
	Reason     sql.NullString
	Suggestion sql.NullString
	CreatedAt  time.Time
	PenaltyAt  sql.NullTime
}

func (product *BannedProduct) SetHash() {
	product.Hash = Hasher(map[string]any{
		"shop_id": product.ShopId,
		"item_id": product.ItemId,
		"status":  product.Status,
	})
}

func (product *BannedProduct) SetStatus(status string) {
	product.Status = status
	product.SetHash()
}

func (product *BannedProduct) HashExist(targets []*BannedProduct) bool {
	for _, target := range targets {
		if target.Hash == product.Hash {
			return true
		}
	}

	return false
}
