package ledisdb

import (
	"github.com/blevesearch/bleve/index/store"
)

type Writer struct {
	store  *Store
	reader *Reader
}

func newWriter(store *Store) (*Writer, error) {
	reader, err := newReader(store)
	if err != nil {
		return nil, err
	}
	return &Writer{
		store:  store,
		reader: reader,
	}, nil
}

func (this *Writer) Set(key, val []byte) error {
	return this.store.set(this.store.keyWithPrefix(key), val)
}

func (this *Writer) Delete(key []byte) error {
	return this.store.del(this.store.keyWithPrefix(key))
}

func (this *Writer) NewBatch() store.KVBatch {
	return store.NewEmulatedBatch(this, this.store.mo)
}

// for reader interface
func (this *Writer) BytesSafeAfterClose() bool {
	return this.reader.BytesSafeAfterClose()
}

func (this *Writer) Get(key []byte) ([]byte, error) {
	return this.reader.Get(key)
}

func (this *Writer) Iterator(key []byte) store.KVIterator {
	return this.reader.Iterator(key)
}

func (this *Writer) Close() error {
	return this.reader.Close()
}
