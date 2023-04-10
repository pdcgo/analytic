package analytic_bigquery_table

import (
	"context"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
)

type AnalyticOrderModel struct {
	Id                int64                 `json:"id" bigquery:"id"`
	ShopId            int64                 `json:"shop_id" bigquery:"shop_id"`
	Hash              string                `json:"hash" bigquery:"hash"`
	SerialNumber      string                `json:"serial_number" bigquery:"serial_number"`
	Status            string                `json:"status" bigquery:"status"`
	Qty               int32                 `json:"qty" bigquery:"qty"`
	ShippingFee       int32                 `json:"shipping_fee" bigquery:"shipping_fee"`
	Subtotal          int32                 `json:"subtotal" bigquery:"subtotal"`
	Total             int32                 `json:"total" bigquery:"total"`
	BuyerId           int64                 `json:"buyer_id" bigquery:"buyer_id"`
	Courier           string                `json:"courier" bigquery:"courier"`
	Resi              bigquery.NullString   `json:"resi" bigquery:"resi"`
	Marketplace       string                `json:"marketplace" bigquery:"marketplace"`
	License           string                `json:"license" bigquery:"license"`
	OrderAt           civil.DateTime        `json:"order_at" bigquery:"order_at"`
	MustSendAt        bigquery.NullDateTime `json:"must_send_at" bigquery:"must_send_at"`
	ShippingAt        bigquery.NullDateTime `json:"shipping_at" bigquery:"shipping_at"`
	ShippingConfirmAt bigquery.NullDateTime `json:"shipping_confirm_at" bigquery:"shipping_confirm_at"`
	FinishedAt        bigquery.NullDateTime `json:"finish_at" bigquery:"finished_at"`
	CanceledAt        bigquery.NullDateTime `json:"canceled_at" bigquery:"canceled_at"`
	CreatedAt         civil.DateTime        `json:"created_at" bigquery:"created_at"`
}

type AnalyticOrderTable struct {
	Maker *BaseTableMaker
}

var analyticOrderSchema = bigquery.Schema{
	{Name: "id", Type: bigquery.IntegerFieldType, Required: true},
	{Name: "shop_id", Type: bigquery.IntegerFieldType, Required: true},
	{Name: "hash", Type: bigquery.StringFieldType, Required: true},
	{Name: "serial_number", Type: bigquery.StringFieldType, Required: true},
	{Name: "status", Type: bigquery.StringFieldType, Required: true},
	{Name: "qty", Type: bigquery.IntegerFieldType, Required: true},
	{Name: "shipping_fee", Type: bigquery.IntegerFieldType, Required: true},
	{Name: "subtotal", Type: bigquery.IntegerFieldType, Required: true},
	{Name: "total", Type: bigquery.IntegerFieldType, Required: true},
	{Name: "buyer_id", Type: bigquery.IntegerFieldType, Required: true},
	{Name: "courier", Type: bigquery.StringFieldType, Required: true},
	{Name: "resi", Type: bigquery.StringFieldType},
	{Name: "marketplace", Type: bigquery.StringFieldType, Required: true},
	{Name: "license", Type: bigquery.StringFieldType, Required: true},
	{Name: "order_at", Type: bigquery.DateTimeFieldType, Required: true},
	{Name: "must_send_at", Type: bigquery.DateTimeFieldType},
	{Name: "shipping_at", Type: bigquery.DateTimeFieldType},
	{Name: "shipping_confirm_at", Type: bigquery.DateTimeFieldType},
	{Name: "finished_at", Type: bigquery.DateTimeFieldType},
	{Name: "canceled_at", Type: bigquery.DateTimeFieldType},
	{Name: "created_at", Type: bigquery.DateTimeFieldType, Required: true},
}

func NewAnalyticOrderTable(opts ...Option) *AnalyticOrderTable {
	o := []Option{
		WithTableName("order"),
		WithSchema(analyticOrderSchema),
	}
	o = append(o, opts...)

	maker := NewTableMaker(o)
	return &AnalyticOrderTable{
		Maker: maker,
	}
}

func (t AnalyticOrderTable) GetTable(ctx context.Context) (*bigquery.Table, error) {
	if !t.Maker.IsExist(ctx) {
		return t.Maker.CreateTable(ctx)
	}

	return t.Maker.GetTableRef(), nil
}

func (t AnalyticOrderTable) InsertRows(ctx context.Context, items []AnalyticOrderModel) error {
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
