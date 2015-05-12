package ledisdb

import (
	"bytes"
	"testing"
)

func TestSmallKey(t *testing.T) {
	pairs := [][][]byte{
		[][]byte{blankKey, blankKey},
		[][]byte{firstKey, blankKey},
		[][]byte{[]byte{0, 0, 0}, []byte{0, 0}},
		[][]byte{[]byte{0, 0, 1, 0}, []byte{0, 0, 0, 255}},
	}

	for _, pair := range pairs {
		if bytes.Compare(pair[0], pair[1]) < 0 {
			t.Fatalf("%s expected to be smaller than %s", pair[0], pair[1])
		}
		if !bytes.Equal(smallerKey(pair[0]), pair[1]) {
			t.Fatalf("smallered %s expected to be equal to %s", pair[0], pair[1])
		}
	}
}
