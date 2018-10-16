package badgercache

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	code := m.Run()
	files, _ := filepath.Glob("test-cache-*")
	for _, f := range files {
		os.RemoveAll(f)
	}
	os.Exit(code)
}

func TestNewCache(t *testing.T) {
	opts := badger.DefaultOptions
	c, err := NewCache(time.Second, &opts, &DefaultGCOptions)
	assert.Nil(t, err)

	assert.Equal(t, time.Second, c.TTL)
	assert.Equal(t, &badger.DefaultOptions, c.opts)
	assert.Equal(t, &DefaultGCOptions, c.gcOpts)
}

func TestSet(t *testing.T) {
	c, err := NewCache(time.Second, nil, nil)
	assert.Nil(t, err)
	conn, err := c.Open("test-cache-set")
	assert.Nil(t, err)
	defer conn.Close()

	key := []byte("set")

	// cache miss
	b := []byte{1, 2, 3}
	err = conn.Write(key, b)
	assert.Nil(t, err)

	// cache hit
	b2, err := conn.Read(key)
	assert.Nil(t, err)
	assert.Equal(t, b, b2)

	// default ttl timeout (cache miss)
	time.Sleep(time.Second)
	_, err = conn.Read(key)
	assert.Errorf(t, err, "Key not found")
}

func TestSetWithTTL(t *testing.T) {
	c, err := NewCache(time.Second, nil, nil)
	assert.Nil(t, err)
	conn, err := c.Open("test-cache-set-ttl")
	assert.Nil(t, err)
	defer conn.Close()

	key := []byte("set")

	// cache miss
	b := []byte{1, 2, 3}
	err = conn.WriteTTL(key, b, time.Second)
	assert.Nil(t, err)

	// cache hit
	b2, err := conn.Read(key)
	assert.Nil(t, err)
	assert.Equal(t, b, b2)

	// default ttl timeout (cache miss)
	time.Sleep(time.Second)
	_, err = conn.Read(key)
	assert.Errorf(t, err, "Key not found")
}

func TestGet(t *testing.T) {
	c, err := NewCache(time.Second, nil, nil)
	assert.Nil(t, err)
	conn, err := c.Open("test-cache-get")
	assert.Nil(t, err)
	defer conn.Close()

	// creates initial key
	key := []byte("my-key")
	// cache miss
	b, err := conn.Read(key)
	assert.Errorf(t, err, "Key not fou")

	// cache hit
	v := []byte{1, 2}
	err = conn.Write(key, v)
	assert.Nil(t, err)
	b, err = conn.Read(key)
	assert.Nil(t, err)
	assert.Equal(t, v, b)
}

func TestBackup(t *testing.T) {
	c, err := NewCache(time.Second, nil, nil)
	assert.Nil(t, err)
	conn, err := c.Open("test-cache-backup")
	assert.Nil(t, err)
	defer conn.Close()

	conn.Write([]byte{1, 2, 3}, []byte{4, 5, 6})
	var b bytes.Buffer
	w := io.Writer(&b)
	upto, err := conn.Backup(w, uint64(time.Now().Add(-1*time.Minute).Unix()))
	assert.Nil(t, err)
	assert.True(t, upto > 0)
}

func TestLoad(t *testing.T) {
	c, err := NewCache(time.Second, nil, nil)
	assert.Nil(t, err)
	conn, err := c.Open("test-cache-load")
	assert.Nil(t, err)
	defer conn.Close()

	conn.Write([]byte{1, 2, 3}, []byte{4, 5, 6})
	var b bytes.Buffer
	w := io.Writer(&b)
	upto, err := conn.Backup(w, uint64(time.Now().Add(-1*time.Minute).Unix()))
	assert.Nil(t, err)
	assert.True(t, upto > 0)

	var readB bytes.Buffer
	r := io.Reader(&readB)
	err = conn.Load(r)
	assert.Nil(t, err)
	assert.Equal(t, b.Bytes(), readB.Bytes())
}

func TestStats(t *testing.T) {
	c, err := NewCache(time.Second, nil, nil)
	assert.Nil(t, err)
	conn, err := c.Open("test-cache-stats")
	assert.Nil(t, err)
	defer conn.Close()

	s, err := conn.Stats()
	assert.Nil(t, err)
	assert.Equal(t, map[string]interface{}{"LSMSize": int64(0), "VLogSize": int64(0)}, s)
}
