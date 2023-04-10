package analytic_bigquery

import (
	"context"

	table "github.com/pdcgo/analytic/analytic_bigquery/table"
)

func RegisterTable(ctx context.Context, tables []table.BaseTableMaker) error {
	for _, table := range tables {
		if !table.IsExist(ctx) {
			_, err := table.CreateTable(ctx)

			if err != nil {
				return err
			}
		}
	}

	return nil
}

func RegisterAnalyticTable(ctx context.Context, opts ...table.Option) error {
	tables := []table.BaseTableMaker{
		*table.NewAnalyticBuyerTable(opts...).Maker,
		*table.NewAnalyticShopTable(opts...).Maker,
		*table.NewAnalyticOrderProductTable(opts...).Maker,
		*table.NewAnalyticOrderTable(opts...).Maker,
		*table.NewBannedProductTable(opts...).Maker,
	}

	return RegisterTable(ctx, tables)
}

func RegisterEventTable(ctx context.Context, opts ...table.Option) error {
	tables := []table.BaseTableMaker{
		*table.NewEventLogTable(opts...).Maker,
	}

	return RegisterTable(ctx, tables)
}
