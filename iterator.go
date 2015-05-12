package ledisdb

import (
	"github.com/tukdesk/ledisdbcli"
)

const (
	defaultScanCount = 100
)

type Iterator struct {
	store *Store

	result *ledisdbcli.KVScanResult
	valid  bool

	idx          int
	currentKey   []byte
	currentValue []byte
}

func newIterator(store *Store) *Iterator {
	return &Iterator{
		store: store,
	}
}

func (this *Iterator) SeekFirst() {
	this.Seek(firstKey)
}

func (this *Iterator) Seek(key []byte) {
	this.loadFrom(this.store.keyWithPrefix(smallerKey(key)), true)
}

func (this *Iterator) loadFrom(cursor []byte, isSeek bool) {
	this.resetCurrent()

	res, err := this.store.scan(cursor, defaultScanCount)
	if err != nil {
		this.valid = false
		return
	}

	this.result = res
	this.valid = res.Len() > 0
	if isSeek {
		this.Next()
	}
}

func (this *Iterator) resetCurrent() {
	this.idx = 0
	this.currentKey = blankKey
	this.currentValue = blankKey
}

func (this *Iterator) Next() {
	// if invalid
	if !this.valid {
		return
	}

	// load more if idx out of range
	if this.idx >= this.result.Len() {
		// set invalid
		this.valid = false
		if this.result.HasMore() {
			this.loadFrom(this.result.Cursor(), false)
		}
	}

	if !this.valid {
		return
	}

	// get value
	key := this.result.Keys()[this.idx]
	value, err := this.store.get(key)
	if err != nil && !ledisdbcli.IsNotFound(err) {
		this.valid = false
		return
	}

	this.currentKey = this.store.keyStripPrefix(key)
	this.currentValue = value
	this.idx++
}

func (this *Iterator) Current() ([]byte, []byte, bool) {
	return this.Key(), this.Value(), this.Valid()
}

func (this *Iterator) Key() []byte {
	if this.valid {
		return this.currentKey
	}
	return nil
}

func (this *Iterator) Value() []byte {
	if this.valid {
		return this.currentValue
	}
	return nil
}

func (this *Iterator) Valid() bool {
	return this.valid
}

func (this *Iterator) Close() error {
	return nil
}
