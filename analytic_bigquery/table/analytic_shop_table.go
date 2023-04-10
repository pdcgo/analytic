package analytic_bigquery_table

import (
	"context"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
)

type AnalyticShopModel struct {
	Id          int64               `json:"id" bigquery:"id"`
	Hash        string              `json:"hash" bigquery:"hash"`
	Name        string              `json:"name" bigquery:"name"`
	Marketplace string              `json:"marketplace" bigquery:"marketplace"`
	Phone       bigquery.NullString `json:"phone" bigquery:"phone"`
	State       bigquery.NullString `json:"state" bigquery:"state"`
	City        bigquery.NullString `json:"city" bigquery:"city"`
	District    bigquery.NullString `json:"district" bigquery:"district"`
	ZipCode     bigquery.NullString `json:"zipcode" bigquery:"zipcode"`
	CreatedAt   civil.DateTime      `json:"created_at" bigquery:"created_at"`
}

type AnalyticShopTable struct {
	Maker *BaseTableMaker
}

var analyticShopSchema = bigquery.Schema{
	{Name: "id", Type: bigquery.IntegerFieldType, Required: true},
	{Name: "hash", Type: bigquery.StringFieldType, Required: true},
	{Name: "name", Type: bigquery.StringFieldType, Required: true},
	{Name: "marketplace", Type: bigquery.StringFieldType, Required: true},
	{Name: "phone", Type: bigquery.StringFieldType},
	{Name: "state", Type: bigquery.StringFieldType},
	{Name: "city", Type: bigquery.StringFieldType},
	{Name: "district", Type: bigquery.StringFieldType},
	{Name: "zipcode", Type: bigquery.StringFieldType},
	{Name: "created_at", Type: bigquery.DateTimeFieldType, Required: true},
}

func NewAnalyticShopTable(opts ...Option) *AnalyticShopTable {
	o := []Option{
		WithTableName("shop"),
		WithSchema(analyticShopSchema),
	}
	o = append(o, opts...)

	maker := NewTableMaker(o)
	return &AnalyticShopTable{
		Maker: maker,
	}
}

func (t AnalyticShopTable) GetTable(ctx context.Context) (*bigquery.Table, error) {
	if !t.Maker.IsExist(ctx) {
		return t.Maker.CreateTable(ctx)
	}

	return t.Maker.GetTableRef(), nil
}

func (t AnalyticShopTable) InsertRows(ctx context.Context, items []AnalyticShopModel) error {
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
