package badgercache

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUint64ToBytes(t *testing.T) {
	num := uint64(1292)
	b := uint64ToBytes(1292)
	i := bytesToUint64(b)
	assert.Equal(t, num, i)
}

func TestBytesToUint64(t *testing.T) {
	bytes := []byte{53, 93, 12, 33, 2, 12, 5, 22}
	i := bytesToUint64(bytes)
	b := uint64ToBytes(i)
	assert.Equal(t, bytes, b)
}

func TestAdd(t *testing.T) {
	b1 := []byte{53, 93, 12, 33, 2, 12, 5, 22}
	b2 := []byte{53, 93, 12, 33, 2, 12, 5, 22}

	i1 := bytesToUint64(b1)
	i2 := bytesToUint64(b2)

	assert.Equal(t, uint64ToBytes(i1+i2), add(b1, b2))
}
