package analytic_bigquery

import (
	"cloud.google.com/go/bigquery"
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

func NewClient(credential []byte, ctx context.Context, opts ...Option) (*bigquery.Client, error) {
	c := &DataClient{
		ProjectID: "shopeepdc",
		ClientOpts: []option.ClientOption{
			option.WithCredentialsJSON(credential),
		},
	}

	for _, opt := range opts {
		opt(c)
	}

	return bigquery.NewClient(ctx, c.ProjectID, c.ClientOpts...)
}
