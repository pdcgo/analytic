package sync_test

import (
	"testing"

	"github.com/pdcgo/analytic/sync"
)

func panicIfError(err error) {
	if err != nil {
		panic(err)
	}
}

type TestItem struct {
	Hash   string `gorm:"hash"`
	Synced bool
}

func (t *TestItem) TableName() string {
	return "test"
}

func (t *TestItem) GetHash() string {
	return t.Hash
}

func TestSync(t *testing.T) {
	t.Skip()
	logger := sync.NewLogger()
	db, err := sync.NewDatabase(".db", logger)
	panicIfError(err)

	item := TestItem{
		Hash:   "hash",
		Synced: true,
	}

	client := sync.NewSyncClient(db)
	err = client.Add(&item)
	panicIfError(err)
}
