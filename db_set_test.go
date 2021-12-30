package CaskDB

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestDB_SAdd(t *testing.T) {
	os.RemoveAll("/tmp/CaskDB")

	db, err := Open(DefaultConfig())
	assert.Nil(t, err)

	k := []byte("name")
	v1 := []byte("zhang san")
	v2 := []byte("li si")

	err = db.SAdd(k, v1, v2)
	assert.Nil(t, err)

	b := db.SIsMember(k, v1)
	assert.True(t, b)

	b = db.SIsMember(k, v2)
	assert.True(t, b)

	err = db.Close()
	assert.Nil(t, err)
}

func TestDB_SRem(t *testing.T) {
	os.RemoveAll("/tmp/CaskDB")

	db, err := Open(DefaultConfig())
	assert.Nil(t, err)

	k := []byte("name")
	v1 := []byte("zhang san")
	v2 := []byte("li si")

	err = db.SAdd(k, v1, v2)
	assert.Nil(t, err)

	err = db.SRem(k, v1)
	assert.Nil(t, err)

	err = db.Close()
	assert.Nil(t, err)
}
func TestDB_SMove(t *testing.T) {
	os.RemoveAll("/tmp/CaskDB")

	db, err := Open(DefaultConfig())
	assert.Nil(t, err)

	k1 := []byte("name")
	k2 := []byte("person")
	v := []byte("zhang san")

	err = db.SAdd(k1, v)
	assert.Nil(t, err)

	err = db.SMove(k1, k2, v)
	assert.Nil(t, err)

	b := db.SIsMember(k1, v)
	assert.False(t, b)

	b = db.SIsMember(k2, v)
	assert.True(t, b)

	err = db.Close()
	assert.Nil(t, err)
}

func TestDB_SUnion(t *testing.T) {
	os.RemoveAll("/tmp/CaskDB")

	db, err := Open(DefaultConfig())
	assert.Nil(t, err)

	k1 := []byte("name")
	k2 := []byte("person")
	v := []byte("zhang san")

	err = db.SAdd(k1, v)
	assert.Nil(t, err)

	err = db.SAdd(k2, v)
	assert.Nil(t, err)

	u, err := db.SUnion(k1, k2)
	assert.Nil(t, err)
	assert.Equal(t, v, u[0])

	err = db.Close()
	assert.Nil(t, err)
}

func TestDB_SDiff(t *testing.T) {
	os.RemoveAll("/tmp/CaskDB")

	db, err := Open(DefaultConfig())
	assert.Nil(t, err)

	k1 := []byte("set1")
	v1 := []byte("a")
	v2 := []byte("b")

	s1 := []byte("set2")
	r1 := []byte("a")
	r2 := []byte("b")
	r3 := []byte("c")

	err = db.SAdd(k1, v1, v2)
	assert.Nil(t, err)

	err = db.SAdd(s1, r1, r2, r3)
	assert.Nil(t, err)

	d, err := db.SDiff(k1, s1)
	assert.Nil(t, err)
	assert.Equal(t, r3, d[0])

	err = db.Close()
	assert.Nil(t, err)
}

func TestDB_SScan(t *testing.T) {
	os.RemoveAll("/tmp/CaskDB")

	db, err := Open(DefaultConfig())
	assert.Nil(t, err)
	k := []byte("k")

	err = db.SAdd(k, []byte("v1"), []byte("v2"))
	res, err := db.SScan(k)
	fmt.Print(string(res[0]), string(res[1]))

	assert.Equal(t, 2, db.SCard(k))
	assert.True(t, db.SIsMember(k, []byte("v1")))
	assert.False(t, db.SIsMember(k, []byte("v3")))

	err = db.Close()
	assert.Nil(t, err)
}

func TestDB_SSet(t *testing.T) {
	os.RemoveAll("/tmp/CaskDB")

	k1 := []byte("k1")
	k2 := []byte("k2")

	for i := 0; i < 2; i++ {
		if i == 0 {
			db, err := Open(DefaultConfig())
			assert.Nil(t, err)

			err = db.SAdd(k1, []byte("a"), []byte("b"))
			err = db.SRem(k1, []byte("a"))
			err = db.SMove(k1, k2, []byte("b"))

			err = db.Close()
			assert.Nil(t, err)
		} else {
			db, err := Open(DefaultConfig())
			assert.Nil(t, err)

			assert.Equal(t, 1, db.SCard(k2))

			err = db.Close()
			assert.Nil(t, err)
		}
	}
}
