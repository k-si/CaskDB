package CaskDB

import "time"

const (
	DefaultDBDir         = "/tmp/CaskDB"
	DefaultMaxKeySize    = 1 * 1024        // 1kb
	DefaultMaxValueSize  = 8 * 1024        // 8kb
	DefaultMaxFileSize   = 1 * 1024 * 1024 // 1mb
	DefaultMergeInterval = 24 * time.Hour
	DefaultWriteSync     = false
	DefaultMultiThread   = false
)

type Config struct {
	DBDir         string
	MaxKeySize    uint32
	MaxValueSize  uint32
	MaxFileSize   int64
	MergeInterval time.Duration
	WriteSync     bool
	MultiThread   bool
}

func DefaultConfig() Config {
	return Config{
		DBDir:         DefaultDBDir,
		MaxKeySize:    DefaultMaxKeySize,
		MaxValueSize:  DefaultMaxValueSize,
		MaxFileSize:   DefaultMaxFileSize,
		MergeInterval: DefaultMergeInterval,
		WriteSync:     DefaultWriteSync,
		MultiThread:   DefaultMultiThread,
	}
}
