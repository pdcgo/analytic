package analytic

import (
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/pdcgo/common_conf/pdc_common"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func GetConnection(dsn string) *gorm.DB {

	Logger := logger.New(
		log.New(os.Stdout, "\r\n", log.LstdFlags),
		logger.Config{
			SlowThreshold:             time.Second,
			LogLevel:                  logger.Silent,
			IgnoreRecordNotFoundError: true,
			Colorful:                  true,
		},
	)

	os.MkdirAll(filepath.Dir(dsn), os.ModeDir)
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{
		Logger: Logger,
	})
	if err != nil {
		panic(err)
	}

	return db
}

type Database struct {
	Connection *gorm.DB
	Models     []any
}

func (db Database) AutoMigrate() error {
	return db.Connection.AutoMigrate(db.Models...)
}

func (db Database) CleanUp() error {
	for _, model := range db.Models {

		tx := db.Connection.Unscoped().Where("1 = 1").Delete(model)
		if tx.Error != nil {
			return tx.Error
		}

		tx = tx.Exec("VACUUM")
		if tx.Error != nil {
			return tx.Error
		}
	}

	return nil
}

func NewAnalyticDB(dsn string) *Database {
	if dsn == "" {
		dsn = ".db/analytic.db"
		oldpath, _ := filepath.Abs(dsn)
		volume := filepath.VolumeName(oldpath)
		newpath := filepath.Join(volume, "/", dsn)
		dsn = newpath

		if _, err := os.Stat(oldpath); err == nil {
			oldfile, _ := os.ReadFile(oldpath)
			newfile, err := os.Create(newpath)
			if err != nil {
				pdc_common.ReportError(err)
			}

			defer newfile.Close()

			_, err = newfile.Write(oldfile)
			if err != nil {
				pdc_common.ReportError(err)
			}

			err = os.RemoveAll(filepath.Dir(oldpath))
			if err != nil {
				pdc_common.ReportError(err)
			}
		}
	}

	return &Database{
		Connection: GetConnection(dsn),
		Models: []any{
			&Buyer{},
			&Shop{},
			&OrderProduct{},
			&Order{},
			&BannedProduct{},
		},
	}
}
