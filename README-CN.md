# 介绍

你好，我是Ksir，一位独立开发者。我在偶然间学习了一点go语言，并在寻求自学资料时，发现麻省理工大学mit6.824-2020春季课程是使用go语言教学的。这让我对分布式和底层原理一下子产生了兴趣，在之后我了解了更多网络和底层存储的知识，并决定锻炼自己完成一项简单的kv数据库。

CaskDB是快速、可内嵌、易维护的k-v数据库引擎，基于Bitcask模型并使用golang实现。 它目前支持String，List、Hash、Set、ZSet五种数据结构。支持客户端连接， 和在您的go项目中内嵌使用。

String类型存于磁盘文件，访问时需要随机读磁盘。其他四种类型除了追加写入磁盘外，还使用内存作为缓存提供高速计算。其中String类型的内存索引使用了AVL Tree，待后续更新我会尝试替换为Red-Black Tree。

# 使用方式

### 命令行

[CaskDB-net](https://github.com/k-si/CaskDB-net) 是使用[Kinx框架](https://github.com/k-si/Kinx) 编写的tcp服务端和客户端。

进入CaskDB-net/server文件夹下：
![Image text](https://ksir-oss.oss-cn-beijing.aliyuncs.com/github/caskdb/caskdb-server.png)

进入CaskDB-net/client文件夹下：
![Image text](https://ksir-oss.oss-cn-beijing.aliyuncs.com/github/caskdb/caskdb-client.png)

### 内嵌入代码

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

### 2,500,000次读写

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
