# 介绍

你好，我是Ksir，一位独立开发者。我在偶然间学习了一点go语言，并在寻求自学资料时，发现麻省理工大学mit6.824-2020春季课程是使用go语言教学的。这让我对分布式和底层原理一下子产生了兴趣，在之后我了解了更多网络和底层存储的知识，并决定锻炼自己完成一项简单的kv数据库。

CaskDB是快速、可内嵌、易维护的k-v数据库引擎，基于Bitcask模型并使用golang实现。 它目前支持String，List、Hash、Set、ZSet五种数据结构。支持客户端连接， 和在您的go项目中内嵌使用。

# 使用方式

1、命令行

2、内嵌入代码

# 性能指标

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
