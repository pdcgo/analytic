package analytic_bigquery_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/pdcgo/analytic/analytic"
	"github.com/pdcgo/common_conf/pdc_common"
	"github.com/stretchr/testify/assert"
)

var AnalyticDB *analytic.Database

func AssertError(t *testing.T, err error) {
	msg := ""
	if err != nil {
		msg = err.Error()
	}

	assert.Nilf(t, err, msg)
}

func getCredByte() []byte {
	base, _ := os.Getwd()
	pathdata := filepath.Join(base, "../logger_credentials.json")

	data, err := os.ReadFile(pathdata)

	if err != nil {
		panic(err)
	}

	return data
}

func TestMain(m *testing.M) {

	pdc_common.SetConfig("../test/assets/config.json", "test", "golang_shopee_test", getCredByte())
	pdc_common.InitializeLogger()

	AnalyticDB = analytic.NewAnalyticDB("../test/.db/test_analytic.db")
	AnalyticDB.AutoMigrate()

	sqlDB, _ := AnalyticDB.Connection.DB()
	defer sqlDB.Close()

	m.Run()
}
