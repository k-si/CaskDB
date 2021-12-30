package CaskDB

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestDB_Push_Pop(t *testing.T) {
	os.RemoveAll("/tmp/CaskDB")

	db, err := Open(DefaultConfig())
	assert.Nil(t, err)

	k := []byte("name")
	v1 := []byte("a")
	v2 := []byte("b")

	// exception test
	v, err := db.RPop(k)
	assert.Nil(t, v)

	v, err = db.LPop(k)
	assert.Nil(t, v)

	// push pop test
	err = db.LPush(k, v2, v1)
	err = db.RPush(k, v1, v2)
	assert.Nil(t, err)

	v, err = db.LPop(k)
	assert.Equal(t, v1, v)
	assert.Nil(t, err)

	v, err = db.RPop(k)
	assert.Equal(t, v2, v)
	assert.Nil(t, err)

	err = db.Close()
	assert.Nil(t, err)
}

func TestDB_LRem(t *testing.T) {
	os.RemoveAll("/tmp/CaskDB")

	db, err := Open(DefaultConfig())
	assert.Nil(t, err)

	k := []byte("name")
	v1 := []byte("a")
	v2 := []byte("b")

	err = db.LPush(k, v1, v1)
	err = db.RPush(k, v2, v2)
	err = db.LPush(k, v2)
	err = db.RPush(k, v1)
	assert.Nil(t, err)

	err = db.LRem(k, v1, 2)
	err = db.LRem(k, v2, -2)
	assert.Nil(t, err)

	v, err := db.LPop(k)
	//log.Print(string(v))
	assert.Equal(t, v2, v)
	assert.Nil(t, err)

	v, err = db.RPop(k)
	assert.Equal(t, v1, v)
	assert.Nil(t, err)

	err = db.Close()
	assert.Nil(t, err)
}

func TestDB_LInsert_RInsert(t *testing.T) {
	os.RemoveAll("/tmp/CaskDB")

	db, err := Open(DefaultConfig())
	assert.Nil(t, err)

	k := []byte("k")

	// 0 1 a b 2 c 3
	err = db.RPush(k, []byte("a"), []byte("b"), []byte("c"))
	err = db.LInsert(k, []byte("0"), 0)
	err = db.LRInsert(k, []byte("1"), 0)
	err = db.LInsert(k, []byte("2"), 4)
	err = db.LRInsert(k, []byte("3"), 5)
	res, err := db.LRange(k, 0, -1)

	assert.Nil(t, err)
	assert.Equal(t, []byte("0"), res[0])
	assert.Equal(t, []byte("1"), res[1])
	assert.Equal(t, []byte("a"), res[2])
	assert.Equal(t, []byte("b"), res[3])
	assert.Equal(t, []byte("2"), res[4])
	assert.Equal(t, []byte("c"), res[5])
	assert.Equal(t, []byte("3"), res[6])

	err = db.Close()
	assert.Nil(t, err)
}

func TestDB_LIndex(t *testing.T) {
	os.RemoveAll("/tmp/CaskDB")

	db, err := Open(DefaultConfig())
	assert.Nil(t, err)

	k := []byte("k")

	err = db.RPush(k, []byte("a"), []byte("b"), []byte("c"))
	v, err := db.LIndex(k, -1)

	assert.Equal(t, []byte("a"), v)
	v, err = db.LIndex(k, 4)

	assert.Equal(t, []byte("c"), v)
	v, err = db.LIndex(k, 1)

	assert.Equal(t, []byte("b"), v)
	assert.Nil(t, err)

	err = db.Close()
	assert.Nil(t, err)
}

func TestDB_LSet(t *testing.T) {
	os.RemoveAll("/tmp/CaskDB")

	db, err := Open(DefaultConfig())
	assert.Nil(t, err)

	k := []byte("k")

	err = db.RPush(k, []byte("a"), []byte("b"), []byte("c"))
	err = db.LSet(k, []byte("0"), -1)
	err = db.LSet(k, []byte("2"), 3)
	err = db.LSet(k, []byte("1"), 1)
	assert.Nil(t, err)

	res, err := db.LRange(k, 0, -1)
	assert.Equal(t, []byte("0"), res[0])
	assert.Equal(t, []byte("1"), res[1])
	assert.Equal(t, []byte("2"), res[2])

	err = db.Close()
	assert.Nil(t, err)
}

func TestDB_LRange(t *testing.T) {
	os.RemoveAll("/tmp/CaskDB")

	db, err := Open(DefaultConfig())
	assert.Nil(t, err)

	k := []byte("k")

	err = db.RPush(k, []byte("a"), []byte("b"), []byte("c"))

	res, err := db.LRange(k, 0, -1)
	assert.Equal(t, []byte("a"), res[0])
	assert.Equal(t, []byte("b"), res[1])
	assert.Equal(t, []byte("c"), res[2])

	res, err = db.LRange(k, 0, -4)
	assert.Nil(t, res)

	res, err = db.LRange(k, -1, 4)
	assert.Equal(t, []byte("c"), res[0])

	res, err = db.LRange(k, 4, 4)
	assert.Nil(t, res)

	assert.Nil(t, err)

	err = db.Close()
	assert.Nil(t, err)
}

func TestDB_LLen(t *testing.T) {
	os.RemoveAll("/tmp/CaskDB")

	db, err := Open(DefaultConfig())
	assert.Nil(t, err)

	k := []byte("k")

	err = db.RPush(k, []byte("a"), []byte("b"), []byte("c"))
	assert.Nil(t, err)

	assert.Equal(t, 3, db.LLen(k))
	assert.True(t, db.LKeyExist(k))
	assert.True(t, db.LExist(k, []byte("a")))
	assert.False(t, db.LExist(k, []byte("d")))

	err = db.Close()
	assert.Nil(t, err)
}

// check rebuild
func TestDB_List(t *testing.T) {
	os.RemoveAll("/tmp/CaskDB")

	k := []byte("k")

	for i := 0; i < 2; i++ {
		if i == 0 {
			db, err := Open(DefaultConfig())
			assert.Nil(t, err)

			// v2
			err = db.LPush(k, []byte("v1"))
			err = db.RPush(k, []byte("v2"))
			err = db.LInsert(k, []byte("v3"), 1)
			err = db.LRInsert(k, []byte("v4"), 2)
			_, err = db.LPop(k)
			_, err = db.RPop(k)
			err = db.LSet(k, []byte("v"), 0)
			err = db.LRem(k, []byte("v"), 1)

			assert.Equal(t, 1, db.LLen(k))

			err = db.Close()
			assert.Nil(t, err)
		} else {
			db, err := Open(DefaultConfig())
			assert.Nil(t, err)

			res, err := db.LRange(k, 0, -1)
			assert.Equal(t, "v2", string(res[0]))
			assert.Equal(t, 1, db.LLen(k))

			err = db.Close()
			assert.Nil(t, err)
		}
	}
}
