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
the memory index of string type. My personal test shows that the performance is not as good as skiplist. I will try to
replace it with red black tree in subsequent updates.

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