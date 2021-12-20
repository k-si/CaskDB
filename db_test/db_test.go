package db_test

import (
	"CaskDB"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)

// file merge
func TestDB_StartMerge(t *testing.T) {
	os.RemoveAll("/tmp/CaskDB")

	cfg := CaskDB.DefaultConfig()
	cfg.MergeInterval = 5 * time.Second
	cfg.MaxFileSize = 100
	db, err := CaskDB.Open(cfg)
	assert.Nil(t, err)

	// 0.data.str
	err = db.Set([]byte("aa"), []byte("11")) // 40
	err = db.Remove([]byte("aa"))            // 38
	// 1.data.str
	err = db.Set([]byte("aa"), []byte("22")) // 40
	err = db.Set([]byte("bb"), []byte("33")) // 40
	// 2.data.str
	err = db.Set([]byte("cc"), []byte("44")) // 40
	err = db.Remove([]byte("cc"))            // 38
	// 3.data.str
	err = db.Set([]byte("dd"), []byte("55")) // 40
	err = db.Set([]byte("ee"), []byte("66")) // 40
	// 4.data.str
	err = db.Set([]byte("ff"), []byte("77")) // 40

	assert.Nil(t, err)

	// merge
	time.Sleep(9 * time.Second)

	aa, err := db.Get([]byte("aa"))
	bb, err := db.Get([]byte("bb"))
	cc, err := db.Get([]byte("cc"))
	dd, err := db.Get([]byte("dd"))
	ee, err := db.Get([]byte("ee"))
	ff, err := db.Get([]byte("ff"))
	assert.Nil(t, err)
	assert.Equal(t, aa, []byte("22"))
	assert.Equal(t, bb, []byte("33"))
	assert.Nil(t, cc)
	assert.Equal(t, dd, []byte("55"))
	assert.Equal(t, ee, []byte("66"))
	assert.Equal(t, ff, []byte("77"))

	err = db.Close()
	assert.Nil(t, err)
}
