package main

import (
	"flag"
	"runtime"

	"github.com/sheki/rocksiterbench"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	initalWrite := flag.Uint("initial_write", 1000000, "number of rows to write before reading")
	writeThreads := flag.Uint("writer_threads", 10, "number of writer threads")
	numIter := flag.Uint("num_iter", 100, "number of iterators to run")
	path := flag.String("db_path", "", "db path")
	dbType := flag.String("db", "bolt", "bolt or rocks")
	flag.Parse()
	db := loadDB(*path, *dbType)
	defer db.Close()
	go rocksiterbench.RecordDiskUsage(*path)
	writer := rocksiterbench.NewDBWriter(db, *initalWrite, *writeThreads)
	writer.WriteAll()
	scanner := rocksiterbench.NewRangeScanner(db, *numIter)

	// bg writer
	go rocksiterbench.NewDBWriter(db, 1000000, *writeThreads).WriteAll()
	scanner.ScanAll()
}

func loadDB(path, db string) rocksiterbench.DB {
	if db == "rocks" {
		rocks, err := rocksiterbench.NewRocksDB(path)
		if err != nil {
			panic(err)
		}
		return rocks
	} else if db == "bolt" {
		bolt, err := rocksiterbench.NewBoltDB(path)
		if err != nil {
			panic(err)
		}
		return bolt
	}
	panic("unknown db type")
}
