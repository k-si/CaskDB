package CaskDB

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestDB_HSet_HGet(t *testing.T) {
	os.RemoveAll("/tmp/CaskDB")

	for i := 0; i < 3; i++ {
		db, err := Open(DefaultConfig())
		assert.Nil(t, err)

		// 1%2=1 2%2=0 3%2=1, so we can test the repetitive kv
		key, k, v := []byte("key"), []byte("k"), []byte(fmt.Sprintf("%d", i%2))
		err = db.HSet(key, k, v)
		assert.Nil(t, err)

		dest, err := db.HGet(key, k)
		assert.Nil(t, err)
		assert.Equal(t, v, dest)

		err = db.Close()
		assert.Nil(t, err)
	}
}

func TestDB_HSet_HDel(t *testing.T) {
	os.RemoveAll("/tmp/CaskDB")

	for i := 0; i < 2; i++ {
		if i == 0 {
			db, err := Open(DefaultConfig())
			assert.Nil(t, err)

			key, k, v := []byte("key"), []byte("k"), []byte("v")
			err = db.HSet(key, k, v)
			assert.Nil(t, err)

			err = db.HDel(key, k)
			assert.Nil(t, err)

			dest, err := db.HGet(key, k)
			assert.Nil(t, dest)

			err = db.Close()
			assert.Nil(t, err)
		} else {
			// second get
			db, err := Open(DefaultConfig())
			assert.Nil(t, err)

			v, err := db.HGet([]byte("key"), []byte("k"))
			assert.Nil(t, v)
		}
	}
}

func TestDB_HSetNx(t *testing.T) {
	os.RemoveAll("/tmp/CaskDB")
	db, err := Open(DefaultConfig())
	assert.Nil(t, err)

	key := []byte("key")
	k := []byte("k")
	v := []byte("v")

	err = db.HSetNx(key, k, v)
	val, err := db.HGet(key, k)
	assert.Equal(t, v, val)

	err = db.HSetNx(key, k, []byte("val"))
	val, err = db.HGet(key, k)
	assert.Equal(t, v, val)

	err = db.Close()
	assert.Nil(t, err)
}

func TestDB_HGetAll(t *testing.T) {
	os.RemoveAll("/tmp/CaskDB")
	db, err := Open(DefaultConfig())
	assert.Nil(t, err)

	key := []byte("key")
	err = db.HSet(key, []byte("k1"), []byte("v1"))
	err = db.HSet(key, []byte("k2"), []byte("v2"))
	_, err = db.HGetAll(key)

	// k1-v1 k2-v2
	assert.True(t, db.HExist(key, []byte("k1")))
	assert.True(t, db.HExist(key, []byte("k2")))

	err = db.Close()
	assert.Nil(t, err)
}

func TestDB_HExist(t *testing.T) {
	os.RemoveAll("/tmp/CaskDB")
	db, err := Open(DefaultConfig())
	assert.Nil(t, err)

	key := []byte("key")

	b := db.HKeyExist(key)
	assert.False(t, b)

	b = db.HExist(key, []byte("k"))
	assert.False(t, b)

	err = db.HSet(key, []byte("k"), []byte("v"))

	b = db.HKeyExist(key)
	assert.True(t, b)

	b = db.HExist(key, []byte("k"))
	assert.True(t, b)

	assert.Equal(t, 1, db.HLen(key))

	err = db.Close()
	assert.Nil(t, err)
}
