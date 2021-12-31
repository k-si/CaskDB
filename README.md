# Introduction

Hello, I'm ksir, an independent developer. I learned a little go language by chance, and when I was looking for
self-study materials, I found MIT 6.824-2020 spring course is taught in go language. This made me interested in
distributed and underlying principles. After that, I learned more about network and underlying storage, and decided to
exercise myself to complete a simple kV database.

Caskdb is a fast, embeddable and easy to maintain K-V database engine, which is based on bitcask model and implemented
by golang. It currently supports five data structures: string, list, hash, set and Zset. Support client connection and
embedded use in your go project.

# Mode of use

1. Command line

2. Embedded code

# Performance index

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