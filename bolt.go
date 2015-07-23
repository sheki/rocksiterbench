package rocksiterbench

import (
	"bytes"
	"log"

	"github.com/boltdb/bolt"
)

var bucketName = []byte("parse")

type BoltDB struct {
	db *bolt.DB
}

func NewBoltDB(path string) (*BoltDB, error) {
	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		return nil, err
	}
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket(bucketName)
		return err

	})
	if err != nil {
		return nil, err
	}
	return &BoltDB{db: db}, nil
}

func (b *BoltDB) Write(k, v []byte) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		err := b.Put(k, v)
		return err
	})
}

type boltIter struct {
	closer   chan struct{}
	cursor   *bolt.Cursor
	firstVal []byte
	prefix   []byte
}

func (b *boltIter) Next() ([]byte, bool) {
	if b.firstVal != nil {
		r := b.firstVal
		b.firstVal = nil
		return r, true
	}
	k, v := b.cursor.Next()
	if !bytes.HasPrefix(k, b.prefix) {
		return nil, false
	}
	return v, true
}

func (b *boltIter) Err() error {
	return nil
}

func (b *boltIter) Close() {
	close(b.closer)
}

func (b *BoltDB) Iterate(key []byte) Iter {
	ch := make(chan *bolt.Cursor)
	iter := &boltIter{closer: make(chan struct{}), prefix: key}
	go b.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(bucketName).Cursor()
		ch <- c
		<-iter.closer
		return nil
	})
	iter.cursor = <-ch
	_, iter.firstVal = iter.cursor.Seek(key)
	return iter
}

func (b *BoltDB) Close() {
	if err := b.db.Close(); err != nil {
		log.Println(err)
	}
}
