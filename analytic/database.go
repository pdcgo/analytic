package analytic

import (
	"io/fs"
	"log"
	"os"
	"strings"
	"time"

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

	dirs := strings.Split(dsn, "/")
	dir := strings.Join(dirs[0:len(dirs)-1], "/")
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		os.Mkdir(dir, fs.ModeDir)
	}

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
	}

	return nil
}

func NewAnalyticDB(dsn string) *Database {
	if dsn == "" {
		dsn = ".db/analytic.db?_auto_vacuum=1"
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
