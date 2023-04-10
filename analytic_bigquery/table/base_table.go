package analytic_bigquery_table

import (
	"context"
	"net/http"
	"time"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/googleapi"
)

type BaseTable interface {
	GetTable(ctx context.Context) (*bigquery.Table, error)
	InsertRows(ctx context.Context, items []any) error
}

type BaseTableMaker struct {
	Client    *bigquery.Client
	Dataset   string
	TableName string
	Schema    bigquery.Schema
}

type Option func(o *BaseTableMaker)

func WithClient(client *bigquery.Client) Option {
	return func(o *BaseTableMaker) {
		o.Client = client
	}
}

func WithDataset(dataset string) Option {
	return func(o *BaseTableMaker) {
		o.Dataset = dataset
	}
}

func WithTableName(tableName string) Option {
	return func(o *BaseTableMaker) {
		o.TableName = tableName
	}
}

func WithSchema(schema bigquery.Schema) Option {
	return func(o *BaseTableMaker) {
		o.Schema = schema
	}
}

func NewTableMaker(opts []Option) *BaseTableMaker {
	tableMaker := &BaseTableMaker{
		Dataset: "analytic",
	}

	for _, opt := range opts {
		opt(tableMaker)
	}

	return tableMaker
}

func (maker *BaseTableMaker) GetTableRef() *bigquery.Table {
	return maker.Client.Dataset(maker.Dataset).Table(maker.TableName)
}

func (maker BaseTableMaker) CreateTable(ctx context.Context) (*bigquery.Table, error) {
	ref := maker.GetTableRef()
	tm := &bigquery.TableMetadata{
		Schema:         maker.Schema,
		ExpirationTime: time.Now().AddDate(1, 0, 0), // NOTE: delete 1 tahun
	}

	if err := ref.Create(ctx, tm); err != nil {
		return ref, err
	}

	return ref, nil
}

func (maker *BaseTableMaker) IsExist(ctx context.Context) bool {
	ref := maker.GetTableRef()

	if _, err := ref.Metadata(ctx); err != nil {
		if e, ok := err.(*googleapi.Error); ok {
			if e.Code == http.StatusNotFound {
				return false
			}
		}
	}

	return true
}
