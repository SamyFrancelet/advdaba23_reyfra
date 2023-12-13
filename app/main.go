package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"regexp"
	"sync"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

type NumberIntCleaner struct {
	r      *bufio.Reader
	buffer string
}

func (nic *NumberIntCleaner) Read(p []byte) (n int, err error) {
	re := regexp.MustCompile(`NumberInt\(([^)]+)\)`)
	for {
		for i := 0; i < 42; i++ {
			var line string
			line, err = nic.r.ReadString('\n')
			if err != nil && err != io.EOF {
				return 0, err
			}

			nic.buffer += line

			if err == io.EOF {
				break
			}
		}

		// Search for a complete NumberInt()
		cleaned := re.ReplaceAllString(nic.buffer, "$1")

		n = copy(p, cleaned)

		if err == io.EOF {
			return n, err
		}

		if n < len(cleaned) {
			nic.buffer = cleaned[n:]
			return n, nil
		}

		if n == len(cleaned) {
			nic.buffer = ""
			return n, nil
		}
	}
}

type Author struct {
	Id   string `json:"_id"`
	Name string `json:"name"`
}

/*type Venue struct {
	Id     string `json:"_id"`
	Name_d string `json:"name_d"`
	Type   int    `json:"type"`
	Raw    string `json:"raw"`
}*/

type Article struct {
	Id         string   `json:"_id"`
	Title      string   `json:"title"`
	Authors    []Author `json:"authors"`
	References []string `json:"references"`

	//NCitations int `json:"n_citation"`

	/*Venue     Venue    `json:"venue"`
	Year      int      `json:"year"`
	Keywords  []string `json:"keywords"`
	Fos       []string `json:"fos"`
	PageStart string   `json:"page_start"`
	PageEnd   string   `json:"page_end"`
	Lang      string   `json:"lang"`
	Volume    string   `json:"volume"`
	Issue     string   `json:"issue"`
	ISSN      string   `json:"issn"`
	ISBN      string   `json:"isbn"`
	DOI       string   `json:"doi"`
	PDF       string   `json:"pdf"`
	URL       []string `json:"url"`
	Abstract  string   `json:"abstract"`*/
}

func (a *Article) ToParams() map[string]interface{} {
	authors := make([]map[string]interface{}, len(a.Authors))
	for i, author := range a.Authors {
		authors[i] = map[string]interface{}{
			"_id":  author.Id,
			"name": author.Name,
		}
	}

	params := map[string]interface{}{
		"_id":        a.Id,
		"title":      a.Title,
		"authors":    authors,
		"references": a.References,
	}

	return params
}

func articlesToParams(articles []Article) map[string]interface{} {
	articlesMap := make([]map[string]interface{}, len(articles))
	for i, article := range articles {
		articlesMap[i] = article.ToParams()
	}

	params := map[string]interface{}{
		"articles": articlesMap,
	}

	return params
}

type DbConfig struct {
	URI      string
	Username string
	Password string
	Query    string
}

var wg sync.WaitGroup

/*func downloadAndParseJson(url string, articles chan Article, max int) {
	resp, err := http.Get(url)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	reader := bufio.NewReader(resp.Body)
	cleaner := &NumberIntCleaner{r: reader}

	err = parseJson(cleaner, dbConf, articles, max)
	if err != nil {
		panic(err)
	}

	wg.Done()
}*/

func readAndParseJson(filepath string, articles chan Article, max int) {
	file, err := os.Open(filepath)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	cleaner := &NumberIntCleaner{r: reader}

	err = parseJson(cleaner, articles, max)
	if err != nil {
		panic(err)
	}

	close(articles)
	//wg.Done()
}

func parseJson(r io.Reader, articles chan Article, max int) error {
	decoder := json.NewDecoder(r)

	// First [
	if _, err := decoder.Token(); err != nil {
		return err
	}

	for i := 0; decoder.More() && i < max; i++ {
		var article Article

		if err := decoder.Decode(&article); err != nil {
			return err
		}

		articles <- article
	}

	// Last ]
	if _, err := decoder.Token(); err != nil {
		return err
	}

	return nil
}

func pushArticlesToDB(dbConf DbConfig, articles chan Article) {
	ctx := context.Background()

	driver, err := neo4j.NewDriverWithContext(
		dbConf.URI,
		neo4j.BasicAuth(dbConf.Username, dbConf.Password, ""),
	)

	if err != nil {
		panic(err)
	}
	defer driver.Close(ctx)

	err = driver.VerifyConnectivity(ctx)
	if err != nil {
		panic(err)
	}

	session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)

	articlesBuf := make([]Article, 0, 10)

	for article := range articles {
		articlesBuf = append(articlesBuf, article)

		if len(articlesBuf) == 10 {
			params := articlesToParams(articlesBuf)
			_, err := session.Run(ctx, dbConf.Query, params)
			if err != nil {
				panic(err)
			}

			articlesBuf = articlesBuf[:0]
		}
	}

	if len(articlesBuf) > 0 {
		params := articlesToParams(articlesBuf)
		_, err := session.Run(ctx, dbConf.Query, params)
		if err != nil {
			panic(err)
		}
	}

	wg.Done()
}

func main() {
	query := `
	UNWIND $articles AS article
	MERGE (a:Article {_id: article._id, title: article.title})

	WITH a, article
	UNWIND article.authors AS author
	MERGE (b:Author {_id: author._id, name: author.name})
	MERGE (b)-[:AUTHORED]->(a)

	WITH a, article
	UNWIND article.references AS ref
	MERGE (c:Article {_id: ref})
	MERGE (a)-[:CITES]->(c)
	`

	/*query := `
	MERGE (a:Article {_id: $article._id, title: $article.title})

	WITH a
	UNWIND $article.authors AS author
	MERGE (b:Author {_id: author._id, name: author.name})
	MERGE (b)-[:AUTHORED]->(a)

	WITH a
	UNWIND $article.references AS ref
	MERGE (c:Article {_id: ref})
	MERGE (a)-[:CITES]->(c)
	`*/

	dbConf := DbConfig{
		URI:      "bolt://3.239.216.81:7687",
		Username: "neo4j",
		Password: "horns-mattresses-cashiers",
		Query:    query,
	}

	articles := make(chan Article, 1000)

	//url := "http://vmrum.isc.heia-fr.ch/biggertest.json"
	//url := "http://vmrum.isc.heia-fr.ch/dblpv13.json"
	filepath := "data/dblpv13.json"

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go pushArticlesToDB(dbConf, articles)
	}

	start := time.Now()

	go readAndParseJson(filepath, articles, 500)
	wg.Wait()

	elapsed := time.Since(start)
	fmt.Printf("Clean + Parse + Add to DB time: %s\n", elapsed)
}
