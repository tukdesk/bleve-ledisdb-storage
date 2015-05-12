#### ledisdb storage for bleve

##### about ledisdb
see [ledisdb](http://ledisdb.com/)

##### about bleve
see [bleve](http://www.blevesearch.com/)

##### installation
```
go get -u github.com/tukdesk/bleve-ledisdb-storage
```

##### usage

```
package main

import (
	"fmt"
	"log"

	"github.com/blevesearch/bleve"
	_ "github.com/tukdesk/bleve-ledisdb-storage"
	// "github.com/tukdesk/ledisdbcli"
)

func main() {
	indexMapping := bleve.NewIndexMapping()

	kvconfig := map[string]interface{}{
		"addr":     "127.0.0.1:6380",
		"password": "",
		"dbIndex":  1,
	}

	// or you can use a shared client
	// client, err := ledisdbcli.New(ledisdbcli.Config{
	// 	Addr:     "127.0.0.1:6380",
	// 	Password: "",
	// 	DBIndex:  1,
	// })

	// if err != nil {
	// 	log.Fatalln(err)
	// }

	// kvconfig := map[string]interface{}{
	// 	"client": client,
	// }

	indexPath := "ledisdbtest"

	index, err := bleve.NewUsing(indexPath, indexMapping, "ledisdb", kvconfig)
	if err != nil && err != bleve.ErrorIndexPathExists {
		log.Fatalln(err)
		return
	}

	if err == nil {
		// initializing
		log.Println("index initializing")
		docs := []struct {
			Title   string
			Content string
			Tags    []string
		}{
			{
				Title:   "first doc",
				Content: "hello world",
				Tags:    []string{"test"},
			},
			{
				Title:   "this is the second doc",
				Content: "I'm using ledisdb storage for bleve",
				Tags:    []string{"example"},
			},
			{
				Title:   "for example",
				Content: "Are you ok mi fans?",
				Tags:    []string{"ledisdb", "bleve"},
			},
		}

		for _, doc := range docs {
			index.Index(doc.Title, doc)
		}
	} else {
		// open existing index
		log.Println("open existing index")
		index, err = bleve.OpenUsing(indexPath, kvconfig)
		if err != nil {
			log.Fatalln(err)
			return
		}
	}

	keywords := []string{"doc", "hello", "bleve", "ledisdb", "fans"}

	for _, word := range keywords {
		query := bleve.NewMatchQuery(word)
		search := bleve.NewSearchRequest(query)
		search.Highlight = bleve.NewHighlight()
		searchResults, err := index.Search(search)
		if err != nil {
			log.Fatalln(err)
			return
		}
		fmt.Printf("result for %s: \n%s \n", word, searchResults)
	}
}

```