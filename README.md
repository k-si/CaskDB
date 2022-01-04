# Introduction

Hello, I'm ksir, an independent developer. I learned a little go language by chance, and when I was looking for
self-study materials, I found MIT 6.824-2020 spring course is taught in go language. This made me interested in
distributed and underlying principles. After that, I learned more about network and underlying storage, and decided to
exercise myself to complete a simple kV database.

Caskdb is a fast, embeddable and easy to maintain K-V database engine, which is based on bitcask model and implemented
by golang. It currently supports five data structures: string, list, hash, set and Zset. Support client connection and
embedded use in your go project.

The string type is stored in the disk file, and the disk needs to be read randomly during access. The other four types
use memory as a cache to provide high-speed computing in addition to additional writing to disk. AVL tree is used for
the memory index of string type. I will try to replace it with red black tree in subsequent updates.

# Mode of use

### Command line

[CaskDB-net](https://github.com/k-si/CaskDB-net) use [kinx](https://github.com/k-si/Kinx) Written TCP server and client.

Enter the caskdb net / server folder:
![Image text]( https://ksir-oss.oss-cn-beijing.aliyuncs.com/github/caskdb/caskdb-server.png)

Enter the caskdb net / client folder:
![Image text]( https://ksir-oss.oss-cn-beijing.aliyuncs.com/github/caskdb/caskdb-client.png)

### Embedded code

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
go test -bench = BenchmarkDB_Set -benchtime = 1000000x -benchmem -run = none
goos: darwin
goarch: arm64
pkg: github.com/k-si/CaskDB
BenchmarkDB_Set-8        1000000              1025 ns/op             520 B/op         10 allocs/op
PASS
ok      github.com/k-si/CaskDB  1.165s


go test -bench = BenchmarkDB_Get -benchtime = 1000000x -benchmem -run = none
goos: darwin
goarch: arm64
pkg: github.com/k-si/CaskDB
BenchmarkDB_Get-8        1000000               134.1 ns/op            24 B/op          1 allocs/op
PASS
ok      github.com/k-si/CaskDB  0.264s
```

### 2,500,000 iterations

```go
go test -bench = BenchmarkDB_Set -benchtime = 2500000x -benchmem -run = none
goos: darwin
goarch: arm64
pkg: github.com/k-si/CaskDB
BenchmarkDB_Set-8        2500000              1040 ns/op             520 B/op         10 allocs/op
PASS
ok      github.com/k-si/CaskDB  2.740s

go test -bench = BenchmarkDB_Get -benchtime = 2500000x -benchmem -run = none
goos: darwin
goarch: arm64
pkg: github.com/k-si/CaskDB
BenchmarkDB_Get-8        2500000               123.1 ns/op            24 B/op          1 allocs/op
PASS
ok      github.com/k-si/CaskDB  0.644s
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