# OmniCache BadgerDB

A [BadgerDB](https://github.com/dgraph-io/badger) persistence layer for [omni-cache](https://github.com/panoplymedia/omni-cache).

### Sample Usage

```go
defaultTimeout := time.Minute
cache, err := NewCache(defaultTimeout, &badger.DefaultOptions, &badgercache.DefaultGCOptions)
if err != nil {
  fmt.Println(err)
}

// open a connection to badger database
conn, err := cache.Open("my-database")
defer conn.Close()

// write data to badger (uses defaultTimeout)
err = conn.Write([]byte("key"), []byte("data"))

// write data to badger with custom timeout
err = conn.WriteTTL([]byte("key2"), []byte("data"), 5*time.Minute)

// read data
data, err := conn.Read([]byte("key"))

// log stats
fmt.Println(conn.Stats())
```
