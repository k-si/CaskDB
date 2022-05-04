# 介绍

CaskDB是基于Bitcask模型实现的kvdb引擎，支持String，List、Hash、Set、ZSet五种类型数据的持久化存储。它的优势如下：
- 日志结构模型，写入速度快
- 多样数据类型支持
- 键值分离存储，对内存依赖较小
- 性能稳定可预测（只需一次内存和一次磁盘访问）

如果你的内存有限，且性能要求不那么高，可以考虑试试CaskDB。

# 使用方式

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

# 基准测试

测试函数详见：CaskDB/db_str_test.go

### 1,000,000次读写

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

### 5,000,000次读写

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

### 10,000,000次读写
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

# 其他

CaskDB支持的数据类型和命令操作如下：

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
