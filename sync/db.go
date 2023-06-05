package sync

import (
	"log"
	"os"
	"path/filepath"
	"time"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func NewLogger() logger.Interface {
	writter := log.New(os.Stdout, "\r\n", log.LstdFlags)
	config := logger.Config{
		SlowThreshold:             time.Second,
		LogLevel:                  logger.Silent,
		IgnoreRecordNotFoundError: true,
		Colorful:                  true,
	}

	return logger.New(writter, config)
}

func NewDatabase(dsn string, logger logger.Interface) (*gorm.DB, error) {
	os.MkdirAll(filepath.Dir(dsn), os.ModeDir)
	config := &gorm.Config{
		Logger: logger,
	}

	return gorm.Open(sqlite.Open(dsn), config)
}
