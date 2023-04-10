package analytic_test

import (
	"testing"

	"github.com/pdcgo/analytic/analytic"
)

var AnalyticDB *analytic.Database

func TestMain(m *testing.M) {
	AnalyticDB = analytic.NewAnalyticDB("../test/.db/test_analytic.db")
	AnalyticDB.AutoMigrate()

	sqlDB, _ := AnalyticDB.Connection.DB()
	defer sqlDB.Close()
	m.Run()
}
