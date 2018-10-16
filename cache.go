package badgercache

import (
	"errors"
	"io"
	"time"

	"github.com/dgraph-io/badger"
)

// Cache contains options to connect to a badger database
// a TTL of 0 does not expire keys
type Cache struct {
	TTL    time.Duration
	opts   *badger.Options
	gcOpts *GarbageCollectionOptions
}

// Conn is a connection to a badger database
type Conn struct {
	TTL    time.Duration
	db     *badger.DB
	ticker *time.Ticker // for GC loop
}

// GarbageCollectionOptions specifies settings for Badger garbage collection
type GarbageCollectionOptions struct {
	Frequency    time.Duration
	DiscardRatio float64
}

// Stats displays stats about badger
type Stats map[string]interface{}

// DefaultGCOptions are the default GarbageCollectionOptions
var DefaultGCOptions = GarbageCollectionOptions{
	Frequency:    time.Minute,
	DiscardRatio: 0.5,
}

// NewCache creates a new Cache
func NewCache(defaultTimeout time.Duration, opts *badger.Options, gcOpts *GarbageCollectionOptions) (*Cache, error) {
	if defaultTimeout < time.Second && defaultTimeout > 0 {
		return &Cache{}, errors.New("TTL must be >= 1 second. Badger uses Unix timestamps for expiries which operate in second resolution")
	}
	if opts == nil {
		opts = &badger.DefaultOptions
	}
	if gcOpts == nil {
		gcOpts = &DefaultGCOptions
	}

	return &Cache{opts: opts, gcOpts: gcOpts, TTL: defaultTimeout}, nil
}

// Open opens a new connection to Badger
func (c Cache) Open(name string) (*Conn, error) {
	c.opts.Dir = name
	c.opts.ValueDir = name

	db, err := badger.Open(*c.opts)
	if err != nil {
		return &Conn{}, err
	}
	// start a GC loop
	ticker := time.NewTicker(c.gcOpts.Frequency)
	go func(t *time.Ticker, d *badger.DB) {
		for range t.C {
		again:
			err := d.RunValueLogGC(c.gcOpts.DiscardRatio)
			if err == nil {
				goto again
			}
		}
	}(ticker, db)
	return &Conn{TTL: c.TTL, ticker: ticker, db: db}, nil
}

// Close closes the badger connection
func (c *Conn) Close() error {
	c.ticker.Stop()
	return c.db.Close()
}

// Write writes data to the cache with the default cache TTL
func (c *Conn) Write(k, v []byte) error {
	return c.WriteTTL(k, v, c.TTL)
}

// WriteTTL writes data to the cache with an explicit TTL
// a TTL of 0 does not expire keys
func (c *Conn) WriteTTL(k, v []byte, ttl time.Duration) error {
	return c.db.Update(func(txn *badger.Txn) error {
		return setWithTTL(txn, k, v, ttl)
	})
}

// Read retrieves data for a key from the cache
func (c *Conn) Read(k []byte) ([]byte, error) {
	ret := []byte{}
	err := c.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(k)
		if err != nil {
			return err
		}
		ret, err = item.Value()
		return err
	})
	return ret, err
}

// Stats provides stats about the Badger database
func (c *Conn) Stats() map[string]interface{} {
	lsm, vlog := c.db.Size()
	return Stats{
		"LSMSize":  lsm,
		"VLogSize": vlog,
	}
}

// Backup dumps a protobuf-encoded list of all entries in the database into the
// given writer, that are newer than the specified version. It returns a
// timestamp indicating when the entries were dumped which can be passed into a
// later invocation to generate an incremental dump, of entries that have been
// added/modified since the last invocation of DB.Backup()
//
// This can be used to backup the data in a database at a given point in time.
func (c *Conn) Backup(w io.Writer, since uint64) (upto uint64, err error) {
	return c.db.Backup(w, since)
}

// Load reads a protobuf-encoded list of all entries from a reader and writes
// them to the database. This can be used to restore the database from a backup
// made by calling DB.Backup().
//
// DB.Load() should be called on a database that is not running any other
// concurrent transactions while it is running.
func (c *Conn) Load(r io.Reader) error {
	return c.db.Load(r)
}

func setWithTTL(txn *badger.Txn, k, v []byte, ttl time.Duration) error {
	// set the new value with TTL
	if ttl < time.Second && ttl > 0 {
		return errors.New("TTL must be >= 1 second. Badger uses Unix timestamps for expiries which operate in second resolution")
	} else if ttl > 0 {
		err := txn.SetWithTTL(k, v, ttl)
		if err != nil {
			return err
		}
	} else {
		err := txn.Set(k, v)
		if err != nil {
			return err
		}
	}
	return nil
}
