package rocksiterbench

import "github.com/tecbot/gorocksdb"

type RocksDB struct {
	db *gorocksdb.DB
}

func NewRocksDB(path string) (*RocksDB, error) {
	opts := gorocksdb.NewDefaultOptions()
	//opts.SetBlockCache(gorocksdb.NewLRUCache(3 << 30))
	opts.SetCreateIfMissing(true)
	db, err := gorocksdb.OpenDb(opts, path)
	return &RocksDB{db: db}, err
}

var wo = gorocksdb.NewDefaultWriteOptions()

func (r *RocksDB) Write(k, v []byte) error {
	return r.db.Put(wo, k, v)
}

func (r *RocksDB) Close() {
	r.db.Close()
}

type rocksIter struct {
	iter *gorocksdb.Iterator
}

func (r *rocksIter) Next() ([]byte, bool) {
	if !r.iter.Valid() {
		return nil, false
	}
	key := r.iter.Key()
	defer key.Free()
	value := r.iter.Value()
	defer value.Free()
	res := value.Data()
	r.iter.Next()
	return res, true
}

func (r *rocksIter) Err() error {
	return r.iter.Err()
}

func (r *rocksIter) Close() {
	r.iter.Close()
}

func (r *RocksDB) Iterate(key []byte) Iter {
	ro := gorocksdb.NewDefaultReadOptions()
	ro.SetFillCache(false)
	it := r.db.NewIterator(ro)
	it.Seek(key)
	return &rocksIter{iter: it}
}
