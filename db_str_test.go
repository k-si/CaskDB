package CaskDB

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestStrSetGet(t *testing.T) {
	os.RemoveAll("/tmp/CaskDB")

	for i := 0; i < 3; i++ {
		db, err := Open(DefaultConfig())
		assert.Nil(t, err)

		// 1%2=1 2%2=0 3%2=1, so we can test the repetitive kv
		k, v := []byte("key"), []byte(fmt.Sprintf("%d", i%2))
		err = db.Set(k, v)
		assert.Nil(t, err)

		dest, err := db.Get(k)
		assert.Nil(t, err)
		assert.Equal(t, dest, v)

		err = db.Close()
		assert.Nil(t, err)
	}
}

func TestStrSetRemove(t *testing.T) {
	os.RemoveAll("/tmp/CaskDB")

	for i := 0; i < 2; i++ {
		if i == 0 {
			// first set remove
			db, err := Open(DefaultConfig())
			assert.Nil(t, err)

			k, v := []byte("key"), []byte("value")
			err = db.Set(k, v)
			assert.Nil(t, err)

			err = db.Remove(k)
			assert.Nil(t, err)

			dest, err := db.Get(k)
			assert.Equal(t, ErrorKeyNotExist, err)
			assert.Nil(t, dest)

			err = db.Close()
			assert.Nil(t, err)
		} else {
			// second get
			db, err := Open(DefaultConfig())
			assert.Nil(t, err)

			v, err := db.Get([]byte("key"))
			assert.Nil(t, v)
			assert.Equal(t, ErrorKeyNotExist, err)
		}
	}
}
