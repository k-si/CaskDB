# Introduction

Caskdb is a kvdb engine based on bitcask model. It supports the persistent storage of five types of data: string, list, hash, set and Zset. Its advantages are as follows:
- Log structure model, fast writing speed
- Multiple data type support
- The key value is stored separately and has little dependence on memory
- Stable and predictable performance (only one memory and one disk access required)

If your memory is limited and your performance requirements are not so high, you can consider trying caskdb.

# Mode of use

```go
package main

import (
	"github.com/k-si/CaskDB"
	"log"
)

func main() {
	db, err := CaskDB.Open(CaskDB.DefaultConfig())
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// do something...
}
```

# Benchmark

### 1,000,000 iterations

```go
go test -bench=BenchmarkDB_Set -benchtime=1000000x -benchmem -run=none
goos: darwin
goarch: arm64
pkg: github.com/k-si/CaskDB
BenchmarkDB_Set-8        1000000              1025 ns/op             520 B/op         10 allocs/op
PASS
ok      github.com/k-si/CaskDB  1.165s

go test -bench=BenchmarkDB_Get -benchtime=1000000x -benchmem -run=none
goos: darwin
goarch: arm64
pkg: github.com/k-si/CaskDB
BenchmarkDB_Get-8        1000000               358.6 ns/op            71 B/op          2 allocs/op
PASS
ok      github.com/k-si/CaskDB  1.516s
```

### 5,000,000 iterations

```go
go test -bench=BenchmarkDB_Set -benchtime=5000000x -benchmem -run=none
goos: darwin
goarch: arm64
pkg: github.com/k-si/CaskDB
BenchmarkDB_Set-8        5000000              1039 ns/op             520 B/op         11 allocs/op
PASS
ok      github.com/k-si/CaskDB  5.647s

go test -bench=BenchmarkDB_Get -benchtime=5000000x -benchmem -run=none
goos: darwin
goarch: arm64
pkg: github.com/k-si/CaskDB
BenchmarkDB_Get-8        5000000               399.5 ns/op            71 B/op          2 allocs/op
PASS
ok      github.com/k-si/CaskDB  7.457s
```

### 10,000,000 iterations
```go
go test -bench=BenchmarkDB_Set -benchtime=10000000x -benchmem -run=none
goos: darwin
goarch: arm64
pkg: github.com/k-si/CaskDB
BenchmarkDB_Set-8       10000000              1058 ns/op             520 B/op         11 allocs/op
PASS
ok      github.com/k-si/CaskDB  11.234s

go test -bench=BenchmarkDB_Get -benchtime=10000000x -benchmem -run=none
goos: darwin
goarch: arm64
pkg: github.com/k-si/CaskDB
BenchmarkDB_Get-8       10000000               404.1 ns/op            71 B/op          2 allocs/op
PASS
ok      github.com/k-si/CaskDB  15.463s
```

# Other

The data types and command operations supported by caskdb are as follows:

- String
    - Set
    - MSet
    - SetNx
    - MSetNx
    - Get
    - MGet
    - GetSet
    - Remove
    - SLen

- Hash
    - HSet
    - HSetNx
    - HGet
    - HGetAll
    - HDel
    - HLen
    - HExist

- List
    - LPush
    - LRPush
    - LPop
    - LRPop
    - LInsert
    - LRInsert
    - LSet
    - LRem
    - LLen
    - LIndex
    - LRange
    - LExist

- Set
    - SAdd
    - SRem
    - SMove
    - SUnion
    - SDiff
    - SScan
    - SCard
    - SIsMember

- ZSet
    - ZAdd
    - ZRem
    - ZScoreRange
    - ZScore
    - ZCard
    - ZIsMember
    - ZTop