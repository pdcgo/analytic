package analytic_bigquery

import (
	"github.com/pdcgo/analytic/analytic"
	table "github.com/pdcgo/analytic/analytic_bigquery/table"
	"golang.org/x/net/context"
)

type BannedProductSync struct {
	Typer        table.TableTypeHandler
	Repo         *analytic.ShopeeBannedProductRepo
	TableOptions []table.Option
}

func NewBannedProductSync(repo *analytic.ShopeeBannedProductRepo, opts ...table.Option) *BannedProductSync {
	return &BannedProductSync{
		Typer:        table.TableTypeHandler{},
		Repo:         repo,
		TableOptions: opts,
	}
}

func (sync BannedProductSync) HashIn(hash string, hashes []string) bool {
	for _, h := range hashes {
		if h == hash {
			return true
		}
	}
	return false
}

func (sync BannedProductSync) ParseBannedProduct(product analytic.BannedProduct) table.BannedProductModel {
	return table.BannedProductModel{
		ShopId:     product.ShopId,
		ItemId:     product.ItemId,
		CatId:      product.CatId,
		Hash:       product.Hash,
		Title:      product.Title,
		Image:      product.Image,
		Status:     product.Status,
		Penalty:    sync.Typer.NullString(product.Penalty.String),
		Reason:     sync.Typer.NullString(product.Reason.String),
		Suggestion: sync.Typer.NullString(product.Suggestion.String),
		CreatedAt:  sync.Typer.DatetimeFromTime(product.CreatedAt),
		PenaltyAt:  sync.Typer.NullDatetimeFromTime(product.PenaltyAt.Time),
	}
}

func (sync BannedProductSync) StoreBannedProducts(ctx context.Context, products []table.BannedProductModel) error {
	hashes := []string{}
	data := []table.BannedProductModel{}
	table := table.NewBannedProductTable(sync.TableOptions...)

	for _, product := range products {
		if !sync.HashIn(product.Hash, hashes) {
			hashes = append(hashes, product.Hash)
			data = append(data, product)
		}
	}

	if len(data) > 0 {
		return table.InsertRows(ctx, data)
	}
	return nil
}

func (sync BannedProductSync) Sync(ctx context.Context) error {
	unsyncProducts, err := sync.Repo.GetUnsyncProducts()
	if err != nil {
		return err
	} else if len(unsyncProducts) == 0 {
		return nil
	}

	products := []table.BannedProductModel{}
	for _, product := range unsyncProducts {
		products = append(products, sync.ParseBannedProduct(product))
	}

	err = sync.StoreBannedProducts(ctx, products)
	if err != nil {
		return err
	}

	err = sync.Repo.SyncProducts()
	if err != nil {
		return err
	}

	return nil
}
