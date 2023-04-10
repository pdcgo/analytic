package analytic_bigquery_table

import (
	"context"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
)

type AnalyticOrderProductModel struct {
	Id               int64               `json:"id" bigquery:"id"`
	OrderId          int64               `json:"order_id" bigquery:"order_id"`
	Hash             string              `json:"hash" bigquery:"hash"`
	Name             string              `json:"name" bigquery:"name"`
	Url              string              `json:"url" bigquery:"url"`
	OrderQty         int32               `json:"order_qty" bigquery:"order_qty"`
	Category         bigquery.NullString `json:"category" bigquery:"category"`
	Image            bigquery.NullString `json:"image" bigquery:"image"`
	Price            int32               `json:"price" bigquery:"price"`
	ActualPrice      int32               `json:"actual_price" bigquery:"actual_price"`
	ProductCreatedAt civil.DateTime      `json:"product_created_at" bigquery:"product_created_at"`
	CreatedAt        civil.DateTime      `json:"created_at" bigquery:"created_at"`
}

type AnalyticOrderProductTable struct {
	Maker *BaseTableMaker
}

var analyticOrderProductSchema = bigquery.Schema{
	{Name: "id", Type: bigquery.IntegerFieldType, Required: true},
	{Name: "order_id", Type: bigquery.IntegerFieldType, Required: true},
	{Name: "hash", Type: bigquery.StringFieldType, Required: true},
	{Name: "name", Type: bigquery.StringFieldType, Required: true},
	{Name: "url", Type: bigquery.StringFieldType, Required: true},
	{Name: "order_qty", Type: bigquery.IntegerFieldType, Required: true},
	{Name: "category", Type: bigquery.StringFieldType},
	{Name: "image", Type: bigquery.StringFieldType},
	{Name: "price", Type: bigquery.IntegerFieldType, Required: true},
	{Name: "actual_price", Type: bigquery.IntegerFieldType, Required: true},
	{Name: "product_created_at", Type: bigquery.DateTimeFieldType, Required: true},
	{Name: "created_at", Type: bigquery.DateTimeFieldType, Required: true},
}

func NewAnalyticOrderProductTable(opts ...Option) *AnalyticOrderProductTable {
	o := []Option{
		WithTableName("order_product"),
		WithSchema(analyticOrderProductSchema),
	}
	o = append(o, opts...)

	maker := NewTableMaker(o)
	return &AnalyticOrderProductTable{
		Maker: maker,
	}
}

func (t AnalyticOrderProductTable) GetTable(ctx context.Context) (*bigquery.Table, error) {
	if !t.Maker.IsExist(ctx) {
		return t.Maker.CreateTable(ctx)
	}

	return t.Maker.GetTableRef(), nil
}

func (t AnalyticOrderProductTable) InsertRows(ctx context.Context, items []AnalyticOrderProductModel) error {
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
