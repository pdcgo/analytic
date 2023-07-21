package analytic

import "cloud.google.com/go/bigquery"

type AnalyticShopeeOrderBigquery struct {
	Client *bigquery.Client
	Repo   ShopeeOrderRepo
}

func (analyticBq AnalyticShopeeOrderBigquery) Test() []Order {
	filter := UnsyncOrderFilter{}
	unsyncOrders, _ := analyticBq.Repo.GetUnsyncOrders(filter)

	return unsyncOrders
}
