package CaskDB

import (
	"time"
)

const (
	DefaultDBDir         = "/tmp/caskdb"
	DefaultMaxKeySize    = 1 * 1024 * 1024  // 1mb
	DefaultMaxValueSize  = 4 * 1024 * 1024  // 4mb
	DefaultMaxFileSize   = 16 * 1024 * 1024 // 16mb
	DefaultMergeInterval = 24 * time.Hour
	DefaultWriteSync     = false
)

type Config struct {
	DBDir         string        `json:"db_dir" yaml:"db_dir" toml:"db_dir"`
	MaxKeySize    uint32        `json:"max_key_size" yaml:"max_key_size" toml:"max_key_size"`
	MaxValueSize  uint32        `json:"max_val_size" yaml:"max_val_size" toml:"max_val_size"`
	MaxFileSize   int64         `json:"max_file_size" yaml:"max_file_size" toml:"max_file_size"`
	MergeInterval time.Duration `json:"gc_interval" yaml:"host" toml:"gc_interval"`
	WriteSync     bool          `json:"sync_now" yaml:"sync_now" toml:"sync_now"`
}

func DefaultConfig() Config {
	return Config{
		DBDir:         DefaultDBDir,
		MaxKeySize:    DefaultMaxKeySize,
		MaxValueSize:  DefaultMaxValueSize,
		MaxFileSize:   DefaultMaxFileSize,
		MergeInterval: DefaultMergeInterval,
		WriteSync:     DefaultWriteSync,
	}
}
