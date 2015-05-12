package ledisdb

import (
	"github.com/blevesearch/bleve/index/store"
)

type Reader struct {
	store *Store
}

func newReader(store *Store) (*Reader, error) {
	return &Reader{
		store: store,
	}, nil
}

func (this *Reader) BytesSafeAfterClose() bool {
	return true
}

func (this *Reader) Get(key []byte) ([]byte, error) {
	return this.store.get(this.store.keyWithPrefix(key))
}

func (this *Reader) Iterator(key []byte) store.KVIterator {
	iter := newIterator(this.store)
	iter.Seek(key)
	return iter
}

func (this *Reader) Close() error {
	return nil
}
