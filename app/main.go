package main

import (
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"
)

var wg sync.WaitGroup

func main() {
	args := os.Args[1:]

	if len(args) != 1 {
		fmt.Println("Usage: ./main <n_articles>")
		os.Exit(1)
	}

	nArticles, err := strconv.Atoi(args[0])
	if err != nil {
		fmt.Println("Usage: ./main <n_articles>")
		os.Exit(1)
	}

	dbConf := DbConfig{
		URI:      "bolt://localhost:7687",
		Username: "neo4j",
		Password: "pizzapizza",
	}

	articles := make(chan Article, 100000)
	authors := make(chan Author, 100000)
	citesRels := make(chan CitesRel, 100000)

	//url := "http://vmrum.isc.heia-fr.ch/biggertest.json"
	url := "http://vmrum.isc.heia-fr.ch/dblpv13.json"
	//filepath := "data/dblpv13.json"

	start := time.Now()

	wg.Add(1)
	go pushArticlesToDB(dbConf, articles)

	//go readAndParseJson(filepath, articles, nil, nil, nArticles)
	go downloadAndParseJson(url, articles, nil, nil, nArticles)
	wg.Wait()

	wg.Add(1)
	go pushAuthorsToDB(dbConf, authors)

	//go readAndParseJson(filepath, nil, authors, nil, nArticles)
	go downloadAndParseJson(url, nil, authors, nil, nArticles)
	wg.Wait()

	wg.Add(1)
	go pushCitesRelsToDB(dbConf, citesRels)

	//go readAndParseJson(filepath, nil, nil, citesRels, nArticles)
	go downloadAndParseJson(url, nil, nil, citesRels, nArticles)
	wg.Wait()

	elapsed := time.Since(start)
	fmt.Printf("Clean + Parse + Add to DB time: %s\n", elapsed)
}
