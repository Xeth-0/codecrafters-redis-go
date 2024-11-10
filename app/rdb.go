package main

import (
// "fmt"
)

type RDBConfig struct {
	dir        string
	dbFileName string
	// ...
}

type redisRDB struct {
	config RDBConfig
	//...
}

func setupRDB(dir string, dbFileName string) redisRDB {
	rdbConfig := RDBConfig{
		dir:        dir,
		dbFileName: dbFileName,
	}

	rdb := redisRDB{
		config: rdbConfig,
	}

	// ...
	return rdb
}
