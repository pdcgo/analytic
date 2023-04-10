package analytic_bigquery

import (
	"time"

	"github.com/google/uuid"
	table "github.com/pdcgo/analytic/analytic_bigquery/table"
	"github.com/pdcgo/common_conf/pdc_common"
	"golang.org/x/net/context"
)

type EventLog struct {
	Config       *pdc_common.PdcConfig
	Bot          string
	Program      string
	TableOptions []table.Option

	typer table.TableTypeHandler
	logs  []table.EventLogModel
}

func NewEventLog(bot string, program string, opts ...table.Option) *EventLog {
	o := []table.Option{
		table.WithDataset("event"),
	}
	o = append(o, opts...)

	return &EventLog{
		Config:       pdc_common.GetConfig(),
		Bot:          bot,
		Program:      program,
		TableOptions: o,
	}
}

func (e *EventLog) StoreLogs(ctx context.Context) error {
	table := table.NewEventLogTable(e.TableOptions...)
	if len(e.logs) > 0 {
		return table.InsertRows(ctx, e.logs)
	}

	return nil
}

func (e *EventLog) CreateLog(args ...string) table.EventLogModel {
	keyArgs := []string{"status", "description"}
	dataArgs := ParseArgs(keyArgs, args, "")

	return table.EventLogModel{
		TrackId:     uuid.New().String(),
		License:     e.Config.Lisensi.Email,
		Version:     e.Config.Version,
		Hostname:    e.Config.Hostname,
		Bot:         e.Bot,
		Program:     e.Program,
		Status:      dataArgs["status"],
		Description: e.typer.NullString(dataArgs["description"]),
		Timestamp:   e.typer.DatetimeFromTime(time.Now()),
	}
}

func (e *EventLog) AddStartLog() {
	log := e.CreateLog("start")
	e.logs = append(e.logs, log)
}

func (e *EventLog) AddErrorLog(err error) {
	log := e.CreateLog("error", err.Error())
	e.logs = append(e.logs, log)
}

func (e *EventLog) AddFinishLog() {
	log := e.CreateLog("finish")
	e.logs = append(e.logs, log)
}
