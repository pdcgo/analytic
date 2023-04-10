package analytic_bigquery_table

import (
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
)

type TableTypeHandler struct{}

func (handler TableTypeHandler) NullString(str string) bigquery.NullString {
	return bigquery.NullString{
		StringVal: str,
		Valid:     str != "",
	}
}

func (handler TableTypeHandler) NullDatetime(datetime civil.DateTime) bigquery.NullDateTime {

	nullTime := bigquery.NullDateTime{
		DateTime: datetime,
		Valid:    !datetime.IsZero(),
	}

	if datetime.Date.Year < 1970 {
		nullTime.Valid = false
	}

	return nullTime
}

func (handler TableTypeHandler) DatetimeFromInt(dt int64) civil.DateTime {
	return civil.DateTimeOf(time.Unix(dt, 0))
}

func (handler TableTypeHandler) DatetimeFromTime(time time.Time) civil.DateTime {
	return civil.DateTimeOf(time)
}

func (handler TableTypeHandler) NullDatetimeFromInt(dt int64) bigquery.NullDateTime {

	datetime := handler.DatetimeFromInt(dt)
	return handler.NullDatetime(datetime)
}

func (handler TableTypeHandler) NullDatetimeFromTime(tm time.Time) bigquery.NullDateTime {

	datetime := handler.DatetimeFromTime(tm)
	return handler.NullDatetime(datetime)
}
