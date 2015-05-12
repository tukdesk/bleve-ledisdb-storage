package ledisdb

import (
	"bytes"
)

var (
	blankKey = []byte{}
	firstKey = []byte{0}
)

func smallerKey(key []byte) []byte {
	if bytes.Equal(key, blankKey) || bytes.Equal(key, firstKey) {
		return blankKey
	}

	length := len(key)

	dst := make([]byte, length)
	copy(dst, key)

	allZero := true
	for i := length - 1; i >= 0; i-- {
		dst[i] = dst[i] - 1
		if dst[i] != 255 {
			allZero = false
			break
		}
	}

	if allZero {
		return make([]byte, length-1)
	}
	return dst
}
