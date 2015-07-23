#Rocks Benchmark to find out number of iterators supported
#Building
Checkout RocksDB and run make shared_lib

```bash
$ cd $HOME/rocksdb # assuming rocks is here
$ make shared_lib
$ CGO_CFLAGS="-I/$HOME/rocksdb/include" CGO_LDFLAGS="-L/$HOME/rocksdb" \
  go install --ldflags '-extldflags "-lrt -static"'  \
  -a  github.com/sheki/rocksiterbench/cmd/rocksiterbench
```
