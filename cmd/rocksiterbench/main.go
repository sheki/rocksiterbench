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
	flag.Parse()
	go rocksiterbench.RecordDiskUsage(*path)
	rocks, err := rocksiterbench.NewRocksDB(*path)
	if err != nil {
		panic(err)
	}
	defer rocks.Close()
	writer := rocksiterbench.NewDBWriter(rocks, *initalWrite, *writeThreads)
	writer.WriteAll()
	scanner := rocksiterbench.NewRangeScanner(rocks, *numIter)

	// bg writer
	go rocksiterbench.NewDBWriter(rocks, 1000000, *writeThreads).WriteAll()
	scanner.ScanAll()
}
