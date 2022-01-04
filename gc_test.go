package CaskDB

import (
	"github.com/stretchr/testify/assert"
	"log"
	"os"
	"testing"
	"time"
)

// test data overflow
func TestDB_GC_Str(t *testing.T) {
	log.Println("gc str")
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

// test List snapshot
func TestDB_GC_List(t *testing.T) {
	log.Println("gc list")
	os.RemoveAll("/tmp/CaskDB")

	cfg := DefaultConfig()
	cfg.MergeInterval = 5 * time.Second
	cfg.MaxFileSize = 80
	db, err := Open(cfg)
	assert.Nil(t, err)

	k1 := []byte("k1")
	k2 := []byte("k2")

	// 0.data.list 1.data.list
	err = db.RPush(k1, []byte("aa"), []byte("bb"), []byte("cc")) // 60 + 30

	// 1.data.list 2.data.list
	err = db.LPush(k2, []byte("aa"), []byte("bb"), []byte("cc")) // 30 + 60

	// 3.data.list 4.data.list
	err = db.LRem(k1, []byte("aa"), 1) // 31
	err = db.LRem(k2, []byte("aa"), 1) // 31

	assert.Nil(t, err)

	// merge
	time.Sleep(8 * time.Second)

	res, err := db.LRange(k1, 0, -1)
	// 0.data.list
	assert.Equal(t, "bb", string(res[0])) // 30
	assert.Equal(t, "cc", string(res[1])) // 30

	res, err = db.LRange(k2, 0, -1)
	// 1.data.list
	assert.Equal(t, "cc", string(res[0])) // 30
	assert.Equal(t, "bb", string(res[1])) // 30

	err = db.Close()
	assert.Nil(t, err)
}

func TestDB_GC_Hash(t *testing.T) {
	log.Println("gc hash")
	os.RemoveAll("/tmp/CaskDB")

	cfg := DefaultConfig()
	cfg.MergeInterval = 5 * time.Second
	cfg.MaxFileSize = 80
	db, err := Open(cfg)
	assert.Nil(t, err)

	k := []byte("k1")
	// 26 + 2 + 2 + 2 = 32
	err = db.HSet(k, []byte("k1"), []byte("v1")) // 32 0
	err = db.HSet(k, []byte("k2"), []byte("v2")) // 64 0
	err = db.HSet(k, []byte("k3"), []byte("v3")) // 32 1
	err = db.HSet(k, []byte("k4"), []byte("v4")) // 64 1
	err = db.HDel(k, []byte("k1"))               // 30 2

	assert.Nil(t, err)

	// merge
	time.Sleep(8 * time.Second)

	_, err = db.HGetAll(k)
	assert.False(t, db.HExist(k, []byte("k1")))
	assert.True(t, db.HExist(k, []byte("k2"))) // 32 0
	assert.True(t, db.HExist(k, []byte("k3"))) // 64 0
	assert.True(t, db.HExist(k, []byte("k4"))) // 32 1

	err = db.Close()
	assert.Nil(t, err)
}

func TestDB_GC_Set(t *testing.T) {
	log.Println("gc set")
	os.RemoveAll("/tmp/CaskDB")

	cfg := DefaultConfig()
	cfg.MergeInterval = 5 * time.Second
	cfg.MaxFileSize = 80
	db, err := Open(cfg)
	assert.Nil(t, err)

	k1 := []byte("k1")
	k2 := []byte("k2")
	// 0.data.set
	err = db.SAdd(k1, []byte("v1"), []byte("v2")) // 30 60
	// 1.data.set
	err = db.SAdd(k2, []byte("v1"), []byte("v2")) // 30 60
	// 2.data.set
	err = db.SRem(k2, []byte("v2")) // 30
	// 2.data.set
	err = db.SMove(k2, k1, []byte("v1")) // 62

	assert.Nil(t, err)

	// merge
	time.Sleep(8 * time.Second)

	// 0.data.set 1.data.set
	assert.False(t, db.SIsMember(k2, []byte("v1")))
	assert.True(t, db.SIsMember(k1, []byte("v1"))) // 30
	assert.True(t, db.SIsMember(k1, []byte("v2"))) // 60
	assert.Equal(t, 2, db.SCard(k1))
	assert.Equal(t, 0, db.SCard(k2))

	err = db.Close()
	assert.Nil(t, err)
}

func TestDB_GC_ZSet(t *testing.T) {
	log.Println("gc zset")
	os.RemoveAll("/tmp/CaskDB")

	cfg := DefaultConfig()
	cfg.MergeInterval = 5 * time.Second
	cfg.MaxFileSize = 80
	db, err := Open(cfg)
	assert.Nil(t, err)

	k := []byte("k")

	// 0.data.zset
	err = db.ZAdd(k, 0.1, []byte("a")) // 26 + 1 + 8 + 1 = 36
	err = db.ZAdd(k, 0.1, []byte("b")) // 72
	// 1.data.zset
	err = db.ZAdd(k, 0.2, []byte("c")) // 36
	err = db.ZRem(k, []byte("b"))      // 36 + 28 = 64

	assert.Nil(t, err)

	// merge
	time.Sleep(8 * time.Second)

	// 0.data.zset
	ok, s := db.ZScore(k, []byte("a"))
	assert.Equal(t, 0.1, s)
	ok, s = db.ZScore(k, []byte("c"))
	assert.Equal(t, 0.2, s)
	ok, s = db.ZScore(k, []byte("b"))
	assert.False(t, ok)

	err = db.Close()
	assert.Nil(t, err)
}

func TestDB_GC(t *testing.T) {
	log.Println("gc")
	os.RemoveAll("/tmp/CaskDB")

	cfg := DefaultConfig()
	cfg.MergeInterval = 3 * time.Second

	db, err := Open(cfg)
	assert.Nil(t, err)

	time.Sleep(5 * time.Second)

	err = db.Close()
	assert.Nil(t, err)
}
