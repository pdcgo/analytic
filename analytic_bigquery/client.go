package analytic_bigquery

import (
	"cloud.google.com/go/bigquery"
	"github.com/pdcgo/common_conf/pdc_common"
	"golang.org/x/net/context"
	"google.golang.org/api/option"
)

type DataClient struct {
	ProjectID  string
	ClientOpts []option.ClientOption
}

type Option func(o *DataClient)

func WithProjectID(projectID string) Option {
	return func(o *DataClient) {
		o.ProjectID = projectID
	}
}

func WithClientOptions(opts []option.ClientOption) Option {
	return func(o *DataClient) {
		o.ClientOpts = opts
	}
}

func NewClient(ctx context.Context, opts ...Option) (*bigquery.Client, error) {
	config := pdc_common.GetConfig()
	c := &DataClient{
		ProjectID: "shopeepdc",
		ClientOpts: []option.ClientOption{
			config.CredOption(),
		},
	}

	for _, opt := range opts {
		opt(c)
	}

	return bigquery.NewClient(ctx, c.ProjectID, c.ClientOpts...)
}
