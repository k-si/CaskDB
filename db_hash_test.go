package CaskDB

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestHashSet(t *testing.T) {
	os.RemoveAll("/tmp/CaskDB")

	db, err := Open(DefaultConfig())
	assert.Nil(t, err)

	key, k, v := []byte("key"), []byte("k"), []byte("v")
	err = db.HSet(key, k, v)
	assert.Nil(t, err)
}

func TestHashSetGet(t *testing.T) {
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

func TestHashSetDel(t *testing.T) {
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
			assert.Equal(t, ErrorKeyNotExist, err)
			assert.Nil(t, dest)

			err = db.Close()
			assert.Nil(t, err)
		} else {
			// second get
			db, err := Open(DefaultConfig())
			assert.Nil(t, err)

			v, err := db.HGet([]byte("key"), []byte("k"))
			assert.Nil(t, v)
			assert.Equal(t, ErrorKeyNotExist, err)
		}
	}
}
