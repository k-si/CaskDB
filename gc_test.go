package CaskDB

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)

// test data overflow and
func TestDB_GC_1(t *testing.T) {
	os.RemoveAll("/tmp/CaskDB")

	cfg := DefaultConfig()
	cfg.MergeInterval = 5 * time.Second
	cfg.MaxFileSize = 80
	db, err := Open(cfg)
	assert.Nil(t, err)

	// 0.data.str
	err = db.Set([]byte("aa"), []byte("11")) // 26 + 4 = 30 bytes
	err = db.Remove([]byte("aa"))            // 26 + 2 = 28 bytes
	// 1.data.str
	err = db.Set([]byte("aa"), []byte("22")) // 30
	err = db.Set([]byte("bb"), []byte("33")) // 30
	// 2.data.str
	err = db.Set([]byte("cc"), []byte("44")) // 30
	err = db.Remove([]byte("cc"))            // 28
	// 3.data.str
	err = db.Set([]byte("dd"), []byte("55")) // 30
	err = db.Set([]byte("ee"), []byte("66")) // 30
	// 4.data.str
	err = db.Set([]byte("ff"), []byte("77")) // 30

	assert.Nil(t, err)

	// merge
	time.Sleep(8 * time.Second)

	aa, err := db.Get([]byte("aa"))
	bb, err := db.Get([]byte("bb"))
	cc, err := db.Get([]byte("cc"))
	dd, err := db.Get([]byte("dd"))
	ee, err := db.Get([]byte("ee"))
	ff, err := db.Get([]byte("ff"))
	assert.Nil(t, err)

	// 0.data.str
	assert.Equal(t, aa, []byte("22")) // 30
	assert.Equal(t, bb, []byte("33")) // 30
	assert.Nil(t, cc)
	// 1.data.str
	assert.Equal(t, dd, []byte("55")) // 30
	assert.Equal(t, ee, []byte("66")) // 30
	// 2.data.str
	assert.Equal(t, ff, []byte("77")) // 30

	err = db.Close()
	assert.Nil(t, err)
}
