package analytic

type ShopeeOrderRepo struct {
	*Database
}

func NewShopeeOrderRepo(db *Database) *ShopeeOrderRepo {
	return &ShopeeOrderRepo{
		Database: db,
	}
}

func (repo ShopeeOrderRepo) RemoveDuplicates(orders []Order) []Order {
	if len(orders) == 0 {
		return orders
	}

	duplicateOrders := []*Order{}
	noDuplicateOrders := []Order{}

	for _, o := range orders {
		duplicateOrders = append(duplicateOrders, &o)
	}

	repo.Database.Connection.Find(&duplicateOrders)
	for _, order := range orders {
		if !order.HashExist(duplicateOrders) {
			noDuplicateOrders = append(noDuplicateOrders, order)
		}
	}

	return noDuplicateOrders
}

func (repo ShopeeOrderRepo) GetUnsyncOrders() ([]Order, error) {
	orders := []Order{}
	tx := repo.Database.Connection
	tx = tx.Preload("Buyer").Preload("Shop").Preload("Products")
	tx = tx.Where(map[string]interface{}{"sync": false})
	tx = tx.Find(&orders)

	if tx.Error != nil {
		return orders, tx.Error
	}

	return orders, nil
}

func (repo ShopeeOrderRepo) StoreOrders(orders []Order) error {
	orders = repo.RemoveDuplicates(orders)
	if len(orders) > 0 {

		res := repo.Database.Connection.Create(&orders)
		return res.Error
	}

	return nil
}

func (repo ShopeeOrderRepo) SyncOrders() error {
	db := repo.Database.Connection

	tx := db.Where(map[string]any{"sync": false}).Updates(&Buyer{Sync: true})
	if tx.Error != nil {
		return tx.Error
	}

	tx = db.Where(map[string]any{"sync": false}).Updates(&Shop{Sync: true})
	if tx.Error != nil {
		return tx.Error
	}

	tx = db.Where(map[string]any{"sync": false}).Updates(&OrderProduct{Sync: true})
	if tx.Error != nil {
		return tx.Error
	}

	tx = db.Where(map[string]any{"sync": false}).Updates(&Order{Sync: true})
	if tx.Error != nil {
		return tx.Error
	}

	return nil
}
