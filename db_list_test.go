package CaskDB

import (
	"github.com/stretchr/testify/assert"
	"log"
	"os"
	"testing"
)

func TestListPushPop(t *testing.T) {
	os.RemoveAll("/tmp/CaskDB")

	db, err := Open(DefaultConfig())
	assert.Nil(t, err)

	k := []byte("name")
	v1 := []byte("a")
	v2 := []byte("b")

	// exception test
	v, err := db.RPop(k)
	assert.Equal(t, ErrorKeyNotExist, err)
	assert.Nil(t, v)

	v, err = db.LPop(k)
	assert.Equal(t, ErrorKeyNotExist, err)
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

func TestListRem(t *testing.T) {
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

	r := db.listIndex.idx.GetRecord()
	for p := r[string(k)].Front(); p != nil; p = p.Next() {
		log.Println(string(p.Value.(*Index).value))
	}

	v, err := db.LPop(k)
	log.Print(string(v))
	assert.Equal(t, v2, v)
	assert.Nil(t, err)

	v, err = db.RPop(k)
	assert.Equal(t, v1, v)
	assert.Nil(t, err)

	err = db.Close()
	assert.Nil(t, err)
}
