package analytic

type ShopeeBannedProductRepo struct {
	*Database
}

func NewShopeeBannedProductRepo(db *Database) *ShopeeBannedProductRepo {
	return &ShopeeBannedProductRepo{
		Database: db,
	}
}

func (repo ShopeeBannedProductRepo) RemoveDuplicates(products []BannedProduct) []BannedProduct {
	if len(products) == 0 {
		return products
	}

	duplicateProducts := []*BannedProduct{}
	noDuplicateProducts := []BannedProduct{}

	for _, p := range products {
		duplicateProducts = append(duplicateProducts, &p)
	}

	repo.Database.Connection.Find(&duplicateProducts)
	for _, product := range products {
		if !product.HashExist(duplicateProducts) {
			noDuplicateProducts = append(noDuplicateProducts, product)
		}
	}

	return noDuplicateProducts
}

func (repo ShopeeBannedProductRepo) GetUnsyncProducts() ([]BannedProduct, error) {
	products := []BannedProduct{}
	tx := repo.Database.Connection
	tx = tx.Where(map[string]interface{}{"sync": false})
	tx = tx.Find(&products)

	if tx.Error != nil {
		return products, tx.Error
	}

	return products, nil
}

func (repo ShopeeBannedProductRepo) StoreProducts(products []BannedProduct) error {
	products = repo.RemoveDuplicates(products)
	if len(products) > 0 {
		res := repo.Database.Connection.Create(&products)
		return res.Error
	}

	return nil
}

func (repo ShopeeBannedProductRepo) SyncProducts() error {
	db := repo.Database.Connection

	tx := db.Where(map[string]any{"sync": false}).Updates(&BannedProduct{Sync: true})
	if tx.Error != nil {
		return tx.Error
	}

	return nil
}
