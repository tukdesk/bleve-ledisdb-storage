package ledisdb

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"github.com/blevesearch/bleve/index/store"
	"github.com/siddontang/ledisdb/config"
	"github.com/siddontang/ledisdb/server"
	"github.com/tukdesk/ledisdbcli"
)

const (
	testDBAddr    = "127.0.0.1:16380"
	testDBDataDir = "./_testdata"
)

func setUp(t *testing.T) (*ledisdbcli.Client, *server.App) {
	os.RemoveAll(testDBDataDir)

	ledisCfg := config.NewConfigDefault()
	ledisCfg.Addr = testDBAddr
	ledisCfg.DataDir = testDBDataDir
	app, err := server.NewApp(ledisCfg)

	go app.Run()

	if err != nil {
		t.Fatal(err)
		return nil, nil
	}

	clientCfg := ledisdbcli.Config{
		Addr:    testDBAddr,
		DBIndex: 15,
	}

	client, err := ledisdbcli.New(clientCfg)
	if err != nil {
		t.Fatal(err)
		return nil, nil
	}

	if err := client.FlushDB(); err != nil {
		t.Fatal(err)
		return nil, nil
	}

	return client, app
}

func tearDown(c *ledisdbcli.Client, app *server.App) {
	c.FlushDB()
	c.Close()
	app.Close()
	os.RemoveAll(testDBDataDir)
}

func TestStore(t *testing.T) {
	c, app := setUp(t)
	defer tearDown(c, app)

	config := map[string]interface{}{
		"addr": testDBAddr,
	}

	s, err := New(config)
	if err != nil {
		t.Fatal(err)
		return
	}

	CommonTestKVStore(t, s)
}

func TestStoreWithSharedClient(t *testing.T) {
	c, app := setUp(t)
	defer tearDown(c, app)

	config := map[string]interface{}{
		"client": c,
	}

	s, err := New(config)
	if err != nil {
		t.Fatal(err)
		return
	}

	CommonTestKVStore(t, s)
}

func TestStoreWithPrefix(t *testing.T) {
	c, app := setUp(t)
	defer tearDown(c, app)

	config := map[string]interface{}{
		"client": c,
		"prefix": "prefix",
	}

	s, err := New(config)
	if err != nil {
		t.Fatal(err)
		return
	}

	if len(s.keyWithPrefix([]byte{})) == 0 {
		t.Fatal(err)
		return
	}

	CommonTestKVStore(t, s)
}

func TestStoreScan(t *testing.T) {
	c, app := setUp(t)
	defer tearDown(c, app)

	config := map[string]interface{}{
		"client": c,
		"prefix": "prefix",
	}

	s, err := New(config)
	if err != nil {
		t.Fatal(err)
		return
	}

	writer, err := s.Writer()
	if err != nil {
		t.Fatal(err)
	}

	count := defaultScanCount*2 + 1

	for i := 0; i < count; i++ {
		key, val := makeKeyValForTest(i)
		if err := writer.Set(key, val); err != nil {
			t.Fatal(err)
		}
	}

	reader, err := s.Reader()
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < count; i++ {
		key, val := makeKeyValForTest(i)
		got, err := reader.Get(key)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(val, got) {
			t.Fatalf("expected %s, got %s", val, got)
		}
	}

	// iterator
	start1 := 0
	keyStart1, _ := makeKeyValForTest(start1)
	iter1 := reader.Iterator(keyStart1)
	for i := start1; i < count; i++ {
		key, val := makeKeyValForTest(i)
		gotKey, gotVal, valid := iter1.Current()
		if !valid {
			t.Fatalf("expected to be valid at %d", i)
		}

		if !bytes.Equal(key, gotKey) {
			t.Fatalf("expected key %s, got %s", key, gotKey)
		}

		if !bytes.Equal(val, gotVal) {
			t.Fatalf("expected val %s, got %s", val, gotVal)
		}
		iter1.Next()
	}

	start2 := 197
	keyStart2, _ := makeKeyValForTest(start2)
	iter2 := reader.Iterator(keyStart2)
	for i := start2; i < count; i++ {
		key, val := makeKeyValForTest(i)
		gotKey, gotVal, valid := iter2.Current()
		if !valid {
			t.Fatalf("expected to be valid at %d", i)
		}

		if !bytes.Equal(key, gotKey) {
			t.Fatalf("expected key %s, got %s", key, gotKey)
		}

		if !bytes.Equal(val, gotVal) {
			t.Fatalf("expected val %s, got %s", val, gotVal)
		}
		iter2.Next()
	}
}

func CommonTestKVStore(t *testing.T, s store.KVStore) {

	writer, err := s.Writer()
	if err != nil {
		t.Error(err)
	}
	err = writer.Set([]byte("a"), []byte("val-a"))
	if err != nil {
		t.Fatal(err)
	}
	err = writer.Set([]byte("z"), []byte("val-z"))
	if err != nil {
		t.Fatal(err)
	}
	err = writer.Delete([]byte("z"))
	if err != nil {
		t.Fatal(err)
	}

	batch := writer.NewBatch()
	batch.Set([]byte("b"), []byte("val-b"))
	batch.Set([]byte("c"), []byte("val-c"))
	batch.Set([]byte("d"), []byte("val-d"))
	batch.Set([]byte("e"), []byte("val-e"))
	batch.Set([]byte("f"), []byte("val-f"))
	batch.Set([]byte("g"), []byte("val-g"))
	batch.Set([]byte("h"), []byte("val-h"))
	batch.Set([]byte("i"), []byte("val-i"))
	batch.Set([]byte("j"), []byte("val-j"))

	err = batch.Execute()
	if err != nil {
		t.Fatal(err)
	}
	err = writer.Close()
	if err != nil {
		t.Fatal(err)
	}

	reader, err := s.Reader()
	if err != nil {
		t.Error(err)
	}
	defer func() {
		err := reader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()
	it := reader.Iterator([]byte("b"))
	key, val, valid := it.Current()
	if !valid {
		t.Fatalf("valid false, expected true")
	}
	if string(key) != "b" {
		t.Fatalf("expected key b, got %s", key)
	}
	if string(val) != "val-b" {
		t.Fatalf("expected value val-b, got %s", val)
	}

	it.Next()
	key, val, valid = it.Current()
	if !valid {
		t.Fatalf("valid false, expected true")
	}
	if string(key) != "c" {
		t.Fatalf("expected key c, got %s", key)
	}
	if string(val) != "val-c" {
		t.Fatalf("expected value val-c, got %s", val)
	}

	it.Seek([]byte("i"))
	key, val, valid = it.Current()
	if !valid {
		t.Fatalf("valid false, expected true")
	}
	if string(key) != "i" {
		t.Fatalf("expected key i, got %s", key)
	}
	if string(val) != "val-i" {
		t.Fatalf("expected value val-i, got %s", val)
	}

	err = it.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func makeKeyValForTest(i int) ([]byte, []byte) {
	return []byte(fmt.Sprintf("key%05d", i)), []byte(fmt.Sprintf("val%05d", i))
}
