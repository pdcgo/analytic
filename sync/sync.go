package sync

import (
	"errors"
	"fmt"
	"reflect"

	"gorm.io/gorm"
)

var ErrIsNotSyncItem = errors.New("is not sync item")

type SyncModel struct {
	Hash   string `gorm:"hash"`
	Synced bool
}

type SyncItem interface {
	TableName() string
	GetHash() string
}

type SyncClient struct {
	DB *gorm.DB
}

func NewSyncClient(db *gorm.DB) *SyncClient {
	return &SyncClient{
		DB: db,
	}
}

func (s *SyncClient) Add(item SyncItem) error {
	t := reflect.ValueOf(item)
	fmt.Printf("%+v\n", t.Interface())

	bItem, ok := t.Interface().(SyncModel)
	fmt.Printf("%+v\n", bItem)

	if !ok {
		return ErrIsNotSyncItem
	}

	tx := s.DB.Find(&bItem)
	if tx.Error != nil {
		return tx.Error
	}

	bItem.Synced = item.GetHash() != bItem.Hash

	return nil
}

// Sync() error
// Migrate(item interface{}) error
