package ledisdb

import (
	"github.com/blevesearch/bleve/index/store"
	"github.com/blevesearch/bleve/registry"
	"github.com/tukdesk/ledisdbcli"
)

const (
	Name = "ledisdb"
)

type Store struct {
	mo           store.MergeOperator
	client       *ledisdbcli.Client
	clientShared bool
	prefix       []byte
}

func New(config map[string]interface{}) (*Store, error) {
	var client *ledisdbcli.Client
	clientShared := false

	client, _ = config["client"].(*ledisdbcli.Client)

	var err error

	if client != nil {
		clientShared = true
	} else {
		addr, _ := config["addr"].(string)
		password, _ := config["password"].(string)

		dbIndex := 0
		if dbIndexFloat, ok := config["dbIndex"].(float64); ok {
			dbIndex = int(dbIndexFloat)
		} else {
			dbIndex, _ = config["dbIndex"].(int)
		}

		cfg := ledisdbcli.Config{
			Addr:     addr,
			Password: password,
			DBIndex:  dbIndex,
		}

		client, err = ledisdbcli.New(cfg)
		if err != nil {
			return nil, err
		}
	}

	prefix, _ := config["prefix"].(string)
	return &Store{
		client:       client,
		clientShared: clientShared,
		prefix:       []byte(prefix),
	}, nil
}

func (this *Store) Open() error {
	return this.client.Ping()
}

func (this *Store) SetMergeOperator(mo store.MergeOperator) {
	this.mo = mo
}

func (this *Store) Writer() (store.KVWriter, error) {
	return newWriter(this)
}

func (this *Store) Reader() (store.KVReader, error) {
	return newReader(this)
}

func (this *Store) Close() error {
	if !this.clientShared {
		this.client.Close()
	}
	return nil
}

func (this *Store) keyWithPrefix(key []byte) []byte {
	if len(this.prefix) == 0 {
		return key
	}

	dst := make([]byte, len(this.prefix)+len(key))
	copy(dst, this.prefix)
	if len(key) > 0 {
		copy(dst[len(this.prefix):], key)
	}
	return dst
}

func (this *Store) keyStripPrefix(key []byte) []byte {
	if len(this.prefix) == 0 {
		return key
	}
	return key[len(this.prefix):]
}

func (this *Store) get(key []byte) ([]byte, error) {
	// must return nil without err if key not found
	val, err := this.client.Get(key)
	if err != nil && !ledisdbcli.IsNotFound(err) {
		return nil, err
	}
	return val, nil
}

func (this *Store) set(key, val []byte) error {
	return this.client.Set(key, val)
}

func (this *Store) del(key []byte) error {
	_, err := this.client.Del(key)
	return err
}

func (this *Store) scan(cursor []byte, count int) (*ledisdbcli.KVScanResult, error) {
	return this.client.KVScan(cursor, count)
}

func StoreConstructor(config map[string]interface{}) (store.KVStore, error) {
	s, err := New(config)
	return s, err
}

func init() {
	registry.RegisterKVStore(Name, StoreConstructor)
}
