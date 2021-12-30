package CaskDB

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestDB_Set_Get(t *testing.T) {
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

func TestDB_Set_Remove(t *testing.T) {
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
			assert.Nil(t, dest)

			err = db.Close()
			assert.Nil(t, err)
		} else {
			// second get
			db, err := Open(DefaultConfig())
			assert.Nil(t, err)

			v, err := db.Get([]byte("key"))
			assert.Nil(t, v)

			err = db.Close()
			assert.Nil(t, err)
		}
	}
}

func TestDB_SetNx(t *testing.T) {
	os.RemoveAll("/tmp/CaskDB")
	db, err := Open(DefaultConfig())
	assert.Nil(t, err)

	k := []byte("k")

	err = db.SetNx(k, []byte("v"))
	val, err := db.Get(k)
	assert.Equal(t, []byte("v"), val)

	err = db.SetNx(k, []byte("v1"))
	val, err = db.Get(k)
	assert.Equal(t, []byte("v"), val)

	err = db.Close()
	assert.Nil(t, err)
}

func TestDB_GetSet(t *testing.T) {
	os.RemoveAll("/tmp/CaskDB")
	db, err := Open(DefaultConfig())
	assert.Nil(t, err)

	err = db.Set([]byte("k"), []byte("v"))
	old, err := db.GetSet([]byte("k"), []byte("v1"))
	assert.Equal(t, []byte("v"), old)

	val, err := db.Get([]byte("k"))
	assert.Equal(t, []byte("v1"), val)

	err = db.Close()
	assert.Nil(t, err)
}

func TestDB_MSet_MGet(t *testing.T) {
	os.RemoveAll("/tmp/CaskDB")
	db, err := Open(DefaultConfig())
	assert.Nil(t, err)

	err = db.MSet([]byte("k1"), []byte("v1"), []byte("k2"), []byte("v2"))
	vals, err := db.MGet([]byte("k1"), []byte("k2"))
	assert.Equal(t, []byte("v1"), vals[0])
	assert.Equal(t, []byte("v2"), vals[1])

	err = db.Close()
	assert.Nil(t, err)
}

func TestDB_MSetNx(t *testing.T) {
	os.RemoveAll("/tmp/CaskDB")
	db, err := Open(DefaultConfig())
	assert.Nil(t, err)

	k := []byte("k")

	err = db.MSetNx(k, []byte("v"), k, []byte("v1"))
	val, err := db.Get(k)
	assert.Equal(t, []byte("v"), val)

	err = db.Close()
	assert.Nil(t, err)
}

func TestDB_StrLen(t *testing.T) {
	os.RemoveAll("/tmp/CaskDB")
	db, err := Open(DefaultConfig())
	assert.Nil(t, err)

	err = db.Set([]byte("k"), []byte("v"))
	assert.Equal(t, 1, db.StrLen())

	err = db.Close()
	assert.Nil(t, err)
}
