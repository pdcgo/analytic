package analytic

import (
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"time"

	"gorm.io/gorm"
)

type BaseOrderModel interface {
	SetHash()
}

func Hasher(data any) string {
	text, _ := json.Marshal(data)
	hash := md5.Sum([]byte(text))

	return hex.EncodeToString(hash[:])
}

type Buyer struct {
	gorm.Model
	Id          int64  `gorm:"primaryKey"`
	Hash        string `gorm:"primaryKey"`
	Sync        bool
	Name        string
	Marketplace string
	Phone       sql.NullString
	State       sql.NullString
	City        sql.NullString
	District    sql.NullString
	Zipcode     sql.NullString
	CreatedAt   time.Time `gorm:"autoCreateTime"`
}

func (buyer *Buyer) SetHash() {
	buyer.Hash = Hasher(map[string]any{
		"id":          buyer.Id,
		"name":        buyer.Name,
		"marketplace": "shopee",
	})
}

type Shop struct {
	gorm.Model
	Id          int64  `gorm:"primaryKey"`
	Hash        string `gorm:"primaryKey"`
	Sync        bool
	Name        string
	Marketplace string
	Phone       sql.NullString
	State       sql.NullString
	City        sql.NullString
	District    sql.NullString
	Zipcode     sql.NullString
	CreatedAt   time.Time `gorm:"autoCreateTime"`
}

func (shop *Shop) SetHash() {
	shop.Hash = Hasher(map[string]any{
		"id":          shop.Id,
		"name":        shop.Name,
		"marketplace": "shopee",
	})
}

type OrderProduct struct {
	gorm.Model
	Id               int64 `gorm:"primaryKey"`
	OrderId          int64
	Hash             string `gorm:"primaryKey"`
	Sync             bool
	Name             string
	Url              string
	OrderQty         int32
	Category         sql.NullString
	Image            sql.NullString
	Price            int32
	ActualPrice      int32
	ProductCreatedAt time.Time
	CreatedAt        time.Time `gorm:"autoCreateTime"`

	Order Order `gorm:"foreignKey:OrderId"`
}

func (orderProduct *OrderProduct) SetHash() {
	orderProduct.Hash = Hasher(map[string]any{
		"id":          orderProduct.Id,
		"order_id":    orderProduct.OrderId,
		"marketplace": "shopee",
	})
}

type Order struct {
	gorm.Model
	Id                int64 `gorm:"primaryKey"`
	ShopId            int64
	Hash              string `gorm:"primaryKey"`
	Sync              bool
	SerialNumber      string
	Status            string
	Qty               int32
	ShippingFee       int32
	Subtotal          int32
	Total             int32
	BuyerId           int64
	Courier           string
	Resi              sql.NullString
	Marketplace       string
	License           string
	OrderAt           time.Time
	MustSendAt        sql.NullTime
	ShippingAt        sql.NullTime
	ShippingConfirmAt sql.NullTime
	FinishedAt        sql.NullTime
	CanceledAt        sql.NullTime
	CreatedAt         time.Time `gorm:"autoCreateTime"`

	Buyer    Buyer          `gorm:"foreignKey:BuyerId"`
	Shop     Shop           `gorm:"foreignKey:ShopId"`
	Products []OrderProduct `gorm:"foreignKey:Id;"`
}

func (order *Order) SetHash() {
	order.Hash = Hasher(map[string]any{
		"id":          order.Id,
		"shop_id":     order.ShopId,
		"status":      order.Status,
		"license":     order.License,
		"marketplace": "shopee",
	})
}

func (order *Order) HashExist(targets []*Order) bool {
	for _, target := range targets {
		if target.Hash == order.Hash {
			return true
		}
	}

	return false
}
