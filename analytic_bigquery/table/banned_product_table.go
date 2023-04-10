package analytic_bigquery_table

import (
	"context"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
)

type BannedProductModel struct {
	ShopId     int64                 `json:"shop_id" bigquery:"shop_id"`
	ItemId     int64                 `json:"item_id" bigquery:"item_id"`
	CatId      int32                 `json:"cat_id" bigquery:"cat_id"`
	Hash       string                `json:"hash" bigquery:"hash"`
	Title      string                `json:"title" bigquery:"title"`
	Image      string                `json:"image" bigquery:"image"`
	Status     string                `json:"status" bigquery:"status"`
	Penalty    bigquery.NullString   `json:"penalty" bigquery:"penalty"`
	Reason     bigquery.NullString   `json:"reason" bigquery:"reason"`
	Suggestion bigquery.NullString   `json:"suggestion" bigquery:"suggestion"`
	CreatedAt  civil.DateTime        `json:"created_at" bigquery:"created_at"`
	PenaltyAt  bigquery.NullDateTime `json:"penalty_at" bigquery:"penalty_at"`
}

type BannedProductTable struct {
	Maker *BaseTableMaker
}

var bannedProductSchema = bigquery.Schema{
	{Name: "shop_id", Type: bigquery.IntegerFieldType, Required: true},
	{Name: "item_id", Type: bigquery.IntegerFieldType, Required: true},
	{Name: "cat_id", Type: bigquery.IntegerFieldType, Required: true},
	{Name: "hash", Type: bigquery.StringFieldType, Required: true},
	{Name: "title", Type: bigquery.StringFieldType, Required: true},
	{Name: "image", Type: bigquery.StringFieldType, Required: true},
	{Name: "status", Type: bigquery.StringFieldType, Required: true},
	{Name: "penalty", Type: bigquery.StringFieldType},
	{Name: "reason", Type: bigquery.StringFieldType},
	{Name: "suggestion", Type: bigquery.StringFieldType},
	{Name: "created_at", Type: bigquery.DateTimeFieldType, Required: true},
	{Name: "penalty_at", Type: bigquery.DateTimeFieldType},
}

func NewBannedProductTable(opts ...Option) *BannedProductTable {
	o := []Option{
		WithTableName("banned_product"),
		WithSchema(bannedProductSchema),
	}
	o = append(o, opts...)

	maker := NewTableMaker(o)
	return &BannedProductTable{
		Maker: maker,
	}
}

func (t BannedProductTable) GetTable(ctx context.Context) (*bigquery.Table, error) {
	if !t.Maker.IsExist(ctx) {
		return t.Maker.CreateTable(ctx)
	}

	return t.Maker.GetTableRef(), nil
}

func (t BannedProductTable) InsertRows(ctx context.Context, items []BannedProductModel) error {
	table, err := t.GetTable(ctx)
	if err != nil {
		return err
	}

	inserter := table.Inserter()
	if err := inserter.Put(ctx, items); err != nil {
		return err
	}

	return nil
}
