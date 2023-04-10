package analytic_bigquery_table

import (
	"context"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
)

type EventLogModel struct {
	TrackId     string              `json:"track_id" bigquery:"track_id"`
	License     string              `json:"license" bigquery:"license"`
	Bot         string              `json:"bot" bigquery:"bot"`
	Version     string              `json:"version" bigquery:"version"`
	Program     string              `json:"program" bigquery:"program"`
	Hostname    string              `json:"hostname" bigquery:"hostname"`
	Status      string              `json:"status" bigquery:"status"`
	Description bigquery.NullString `json:"description" bigquery:"description"`
	Timestamp   civil.DateTime      `json:"created_at" bigquery:"created_at"`
}

type EventLogTable struct {
	Maker *BaseTableMaker
}

var eventLogSchema = bigquery.Schema{
	{Name: "track_id", Type: bigquery.StringFieldType, Required: true},
	{Name: "license", Type: bigquery.StringFieldType, Required: true},
	{Name: "bot", Type: bigquery.StringFieldType, Required: true},
	{Name: "version", Type: bigquery.StringFieldType, Required: true},
	{Name: "program", Type: bigquery.StringFieldType, Required: true},
	{Name: "hostname", Type: bigquery.StringFieldType, Required: true},
	{Name: "status", Type: bigquery.StringFieldType, Required: true},
	{Name: "description", Type: bigquery.StringFieldType},
	{Name: "created_at", Type: bigquery.DateTimeFieldType, Required: true},
}

func NewEventLogTable(opts ...Option) *EventLogTable {
	o := []Option{
		WithTableName("log"),
		WithSchema(eventLogSchema),
	}
	o = append(o, opts...)

	maker := NewTableMaker(o)
	return &EventLogTable{
		Maker: maker,
	}
}

func (t EventLogTable) GetTable(ctx context.Context) (*bigquery.Table, error) {
	if !t.Maker.IsExist(ctx) {
		return t.Maker.CreateTable(ctx)
	}

	return t.Maker.GetTableRef(), nil
}

func (t EventLogTable) InsertRows(ctx context.Context, items []EventLogModel) error {
	table, err := t.GetTable(ctx)
	if err != nil {
		return err
	}

	inserter := table.Inserter()
	if err := inserter.Put(ctx, items); err != nil {
		return err
	}

	return nil
}
