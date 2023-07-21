package analytic_bigquery

import (
	"github.com/pdcgo/analytic/analytic"
	table "github.com/pdcgo/analytic/analytic_bigquery/table"
	"golang.org/x/net/context"
)

type BigqueryOrderSync struct {
	Typer        table.TableTypeHandler
	Repo         *analytic.ShopeeOrderRepo
	TableOptions []table.Option
}

func NewBigqueryOrderSync(repo *analytic.ShopeeOrderRepo, opts ...table.Option) *BigqueryOrderSync {
	return &BigqueryOrderSync{
		Typer:        table.TableTypeHandler{},
		Repo:         repo,
		TableOptions: opts,
	}
}

func (sync BigqueryOrderSync) HashIn(hash string, hashes []string) bool {
	for _, h := range hashes {
		if h == hash {
			return true
		}
	}
	return false
}

func (sync BigqueryOrderSync) ParseBuyer(buyer analytic.Buyer) table.AnalyticBuyerModel {
	return table.AnalyticBuyerModel{
		Id:          buyer.Id,
		Hash:        buyer.Hash,
		Name:        buyer.Name,
		Marketplace: buyer.Marketplace,
		Phone:       sync.Typer.NullString(buyer.Phone.String),
		State:       sync.Typer.NullString(buyer.State.String),
		City:        sync.Typer.NullString(buyer.City.String),
		District:    sync.Typer.NullString(buyer.District.String),
		Zipcode:     sync.Typer.NullString(buyer.Zipcode.String),
		CreatedAt:   sync.Typer.DatetimeFromTime(buyer.CreatedAt),
	}
}

func (sync BigqueryOrderSync) ParseShop(shop analytic.Shop) table.AnalyticShopModel {
	return table.AnalyticShopModel{
		Id:          int64(shop.Id),
		Hash:        shop.Hash,
		Name:        shop.Name,
		Marketplace: shop.Marketplace,
		Phone:       sync.Typer.NullString(shop.Phone.String),
		State:       sync.Typer.NullString(shop.State.String),
		City:        sync.Typer.NullString(shop.City.String),
		District:    sync.Typer.NullString(shop.District.String),
		ZipCode:     sync.Typer.NullString(shop.Zipcode.String),
		CreatedAt:   sync.Typer.DatetimeFromTime(shop.CreatedAt),
	}
}

func (sync BigqueryOrderSync) ParseOrder(order analytic.Order) table.AnalyticOrderModel {
	return table.AnalyticOrderModel{
		Id:                order.Id,
		ShopId:            order.ShopId,
		Hash:              order.Hash,
		SerialNumber:      order.SerialNumber,
		Status:            order.Status,
		Qty:               order.Qty,
		ShippingFee:       order.ShippingFee,
		Subtotal:          order.Subtotal,
		Total:             order.Total,
		BuyerId:           order.BuyerId,
		Courier:           order.Courier,
		Resi:              sync.Typer.NullString(order.Resi.String),
		Marketplace:       order.Marketplace,
		License:           order.License,
		OrderAt:           sync.Typer.DatetimeFromTime(order.CreatedAt),
		MustSendAt:        sync.Typer.NullDatetimeFromTime(order.MustSendAt.Time),
		ShippingAt:        sync.Typer.NullDatetimeFromTime(order.ShippingAt.Time),
		ShippingConfirmAt: sync.Typer.NullDatetimeFromTime(order.ShippingConfirmAt.Time),
		FinishedAt:        sync.Typer.NullDatetimeFromTime(order.FinishedAt.Time),
		CanceledAt:        sync.Typer.NullDatetimeFromTime(order.CanceledAt.Time),
		CreatedAt:         sync.Typer.DatetimeFromTime(order.CreatedAt),
	}
}

func (sync BigqueryOrderSync) ParseOrderProduct(orderProduct analytic.OrderProduct) table.AnalyticOrderProductModel {
	return table.AnalyticOrderProductModel{
		Id:               orderProduct.Id,
		OrderId:          orderProduct.OrderId,
		Hash:             orderProduct.Hash,
		Name:             orderProduct.Name,
		Url:              orderProduct.Url,
		OrderQty:         orderProduct.OrderQty,
		Category:         sync.Typer.NullString(orderProduct.Category.String),
		Image:            sync.Typer.NullString(orderProduct.Image.String),
		Price:            orderProduct.Price,
		ActualPrice:      orderProduct.ActualPrice,
		ProductCreatedAt: sync.Typer.DatetimeFromTime(orderProduct.ProductCreatedAt),
		CreatedAt:        sync.Typer.DatetimeFromTime(orderProduct.CreatedAt),
	}
}

func (sync BigqueryOrderSync) StoreBuyers(ctx context.Context, buyers []table.AnalyticBuyerModel) error {
	hashes := []string{}
	data := []table.AnalyticBuyerModel{}
	table := table.NewAnalyticBuyerTable(sync.TableOptions...)

	for _, buyer := range buyers {
		if !sync.HashIn(buyer.Hash, hashes) {
			hashes = append(hashes, buyer.Hash)
			data = append(data, buyer)
		}
	}

	if len(data) > 0 {
		return table.InsertRows(ctx, data)
	}
	return nil
}

func (sync BigqueryOrderSync) StoreShops(ctx context.Context, shops []table.AnalyticShopModel) error {
	hashes := []string{}
	data := []table.AnalyticShopModel{}
	table := table.NewAnalyticShopTable(sync.TableOptions...)

	for _, shop := range shops {
		if !sync.HashIn(shop.Hash, hashes) {
			hashes = append(hashes, shop.Hash)
			data = append(data, shop)
		}
	}

	if len(data) > 0 {
		return table.InsertRows(ctx, data)
	}
	return nil
}

func (sync BigqueryOrderSync) StoreOrderProducts(ctx context.Context, orderProducts []table.AnalyticOrderProductModel) error {
	hashes := []string{}
	data := []table.AnalyticOrderProductModel{}
	table := table.NewAnalyticOrderProductTable(sync.TableOptions...)

	for _, orderProduct := range orderProducts {
		if !sync.HashIn(orderProduct.Hash, hashes) {
			hashes = append(hashes, orderProduct.Hash)
			data = append(data, orderProduct)
		}
	}

	if len(data) > 0 {
		return table.InsertRows(ctx, data)
	}
	return nil
}

func (sync BigqueryOrderSync) StoreOrders(ctx context.Context, orders []table.AnalyticOrderModel) error {
	hashes := []string{}
	data := []table.AnalyticOrderModel{}
	table := table.NewAnalyticOrderTable(sync.TableOptions...)

	for _, order := range orders {
		if !sync.HashIn(order.Hash, hashes) {
			hashes = append(hashes, order.Hash)
			data = append(data, order)
		}
	}

	if len(data) > 0 {
		return table.InsertRows(ctx, data)
	}
	return nil
}

func (sync BigqueryOrderSync) Sync(ctx context.Context, shopid int64) error {
	filter := analytic.UnsyncOrderFilter{
		Shopid: shopid,
	}
	unsyncOrders, err := sync.Repo.GetUnsyncOrders(filter)
	if err != nil {
		return err
	} else if len(unsyncOrders) == 0 {
		return nil
	}

	buyers := []table.AnalyticBuyerModel{}
	shops := []table.AnalyticShopModel{}
	orders := []table.AnalyticOrderModel{}
	orderProducts := []table.AnalyticOrderProductModel{}

	for _, order := range unsyncOrders {
		buyers = append(buyers, sync.ParseBuyer(order.Buyer))
		shops = append(shops, sync.ParseShop(order.Shop))
		orders = append(orders, sync.ParseOrder(order))

		for _, product := range order.Products {
			orderProducts = append(orderProducts, sync.ParseOrderProduct(product))
		}
	}

	err = sync.StoreBuyers(ctx, buyers)
	if err != nil {
		return err
	}

	err = sync.StoreShops(ctx, shops)
	if err != nil {
		return err
	}

	err = sync.StoreOrderProducts(ctx, orderProducts)
	if err != nil {
		return err
	}

	err = sync.StoreOrders(ctx, orders)
	if err != nil {
		return err
	}

	err = sync.Repo.SyncOrders()
	if err != nil {
		return err
	}

	return nil
}
