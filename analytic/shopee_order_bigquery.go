package analytic

import "cloud.google.com/go/bigquery"

type AnalyticShopeeOrderBigquery struct {
	Client *bigquery.Client
	Repo   ShopeeOrderRepo
}

func (analyticBq AnalyticShopeeOrderBigquery) Test() []Order {

	unsyncOrders, _ := analyticBq.Repo.GetUnsyncOrders()

	return unsyncOrders
}
