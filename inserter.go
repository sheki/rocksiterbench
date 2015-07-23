package rocksiterbench

import (
	"fmt"
	"log"
	"sync"
	"time"
)

type DB interface {
	Write(key, value []byte) error
	Iterate(key []byte) Iter
	Close()
}

type Iter interface {
	Next() ([]byte, bool)
	Err() error
	Close()
}

// Writes to a DB
type DBWriter struct {
	db              DB
	numWrites       uint
	parallelWriters uint
}

func NewDBWriter(db DB, numWrites uint, parallelWriters uint) *DBWriter {
	if parallelWriters == 0 {
		return nil
	}
	return &DBWriter{db: db, numWrites: numWrites, parallelWriters: parallelWriters}
}

// WriteAll writes the given number of records. Waits till completion
func (d *DBWriter) WriteAll() {
	wg := sync.WaitGroup{}
	wg.Add(int(d.parallelWriters))
	threadLimit := d.numWrites / d.parallelWriters
	start := time.Now()
	for i := uint(0); i < d.parallelWriters; i++ {
		go func(t uint) {
			defer wg.Done()
			for i := uint(0); i < t; i++ {
				if err := d.db.Write(randKey(15), randBlob(250)); err != nil {
					log.Println(err)
				}
			}
		}(threadLimit)
	}
	wg.Wait()
	runTime := time.Now().Sub(start)
	fmt.Printf(
		"wrote %d records in %f seconds, TPut %f/s Latency %fms/record\n",
		d.numWrites,
		runTime.Seconds(),
		float64(d.numWrites)/runTime.Seconds(),
		runTime.Seconds()*1000/float64(d.numWrites),
	)
}

// RangeScans over a database with the given number of threads
type RangeScanner struct {
	db          DB
	parallelism uint
}

// ScanAll range scans the database with a random two letter key
// waits till all scans complete
func (r *RangeScanner) ScanAll() {
	wg := sync.WaitGroup{}
	wg.Add(int(r.parallelism))
	start := time.Now()
	for i := uint(0); i < r.parallelism; i++ {
		go func() {
			fmt.Println("scnanner started")
			defer wg.Done()
			iter := r.db.Iterate(randKey(2))
			defer iter.Close()
			for {
				_, ok := iter.Next()
				if !ok {
					if err := iter.Err(); err != nil {
						log.Println(err)
					}
					return
				}
			}
		}()
	}
	wg.Wait()
	runTime := time.Now().Sub(start)
	fmt.Printf(
		"read in %f seconds using %d threads\n",
		runTime.Seconds(),
		r.parallelism,
	)
}

func NewRangeScanner(db DB, parallelism uint) *RangeScanner {
	return &RangeScanner{db: db, parallelism: parallelism}
}
