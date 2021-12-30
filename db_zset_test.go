package CaskDB

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestDB_ZAdd(t *testing.T) {
	os.RemoveAll("/tmp/CaskDB")

	k := []byte("k")
	v := []byte("v")
	for i := 0; i < 2; i++ {
		db, err := Open(DefaultConfig())
		assert.Nil(t, err)

		err = db.ZAdd(k, float64(i), v)

		res, err := db.ZTop(k, 1)
		assert.Equal(t, string(v), res[0].(string))
		assert.Equal(t, float64(i), res[1].(float64))

		err = db.Close()
		assert.Nil(t, err)
	}
}

func TestDB_ZRem(t *testing.T) {
	os.RemoveAll("/tmp/CaskDB")

	k := []byte("k")
	v := []byte("v")
	for i := 0; i < 2; i++ {
		if i == 0 {
			db, err := Open(DefaultConfig())
			assert.Nil(t, err)

			err = db.ZAdd(k, float64(i), v)
			err = db.ZRem(k, v)

			err = db.Close()
			assert.Nil(t, err)
		} else {
			db, err := Open(DefaultConfig())
			assert.Nil(t, err)

			assert.False(t, db.ZIsMember(k, v))

			err = db.Close()
			assert.Nil(t, err)
		}
	}
}

func TestDB_ZScoreRange(t *testing.T) {
	os.RemoveAll("/tmp/CaskDB")

	db, err := Open(DefaultConfig())
	assert.Nil(t, err)

	k := []byte("k")

	err = db.ZAdd(k, 50, []byte("v1"))
	err = db.ZAdd(k, 70, []byte("v2"))
	err = db.ZAdd(k, 90, []byte("v3"))

	res, err := db.ZScoreRange(k, 60, 80)
	assert.Equal(t, "v2", res[0].(string))
	assert.Equal(t, float64(70), res[1].(float64))

	err = db.Close()
	assert.Nil(t, err)
}

func TestDB_ZScore(t *testing.T) {
	os.RemoveAll("/tmp/CaskDB")

	db, err := Open(DefaultConfig())
	assert.Nil(t, err)

	k := []byte("k")

	err = db.ZAdd(k, 50, []byte("v1"))
	ok, score := db.ZScore(k, []byte("v1"))
	assert.True(t, ok)
	assert.Equal(t, float64(50), score)

	err = db.Close()
	assert.Nil(t, err)
}
