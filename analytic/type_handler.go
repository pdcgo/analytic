package analytic

import (
	"database/sql"
	"time"
)

type TableTypeHandler struct{}

func (handler TableTypeHandler) NullString(str string) sql.NullString {
	return sql.NullString{
		String: str,
		Valid:  str != "",
	}
}

func (handler TableTypeHandler) NullTime(tm time.Time) sql.NullTime {

	nullTime := sql.NullTime{
		Time:  tm,
		Valid: !tm.IsZero(),
	}

	if tm.Unix() == 0 {
		nullTime.Valid = false
	}

	return nullTime
}
