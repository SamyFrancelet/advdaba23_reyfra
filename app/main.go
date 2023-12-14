package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
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

type AuthoredRel struct {
	AuthorId  string
	ArticleId string
}

func (ar *AuthoredRel) ToParams() map[string]interface{} {
	params := map[string]interface{}{
		"authorId":  ar.AuthorId,
		"articleId": ar.ArticleId,
	}

	return params
}

func authoredRelsToParams(authoredRels []AuthoredRel) map[string]interface{} {
	authoredRelsMap := make([]map[string]interface{}, len(authoredRels))
	for i, authoredRel := range authoredRels {
		authoredRelsMap[i] = authoredRel.ToParams()
	}

	params := map[string]interface{}{
		"authoredRels": authoredRelsMap,
	}

	return params
}

type CitesRel struct {
	ArticleId string
	RefId     string
}

func (cr *CitesRel) ToParams() map[string]interface{} {
	params := map[string]interface{}{
		"articleId": cr.ArticleId,
		"refId":     cr.RefId,
	}

	return params
}

func citesRelsToParams(citesRels []CitesRel) map[string]interface{} {
	citesRelsMap := make([]map[string]interface{}, len(citesRels))
	for i, citesRel := range citesRels {
		citesRelsMap[i] = citesRel.ToParams()
	}

	params := map[string]interface{}{
		"citesRels": citesRelsMap,
	}

	return params
}

type Author struct {
	Id   string `json:"_id"`
	Name string `json:"name"`
}

func (a *Author) ToParams() map[string]interface{} {
	params := map[string]interface{}{
		"_id":  a.Id,
		"name": a.Name,
	}

	return params
}

func authorsToParams(authors []Author) map[string]interface{} {
	authorsMap := make([]map[string]interface{}, len(authors))
	for i, author := range authors {
		authorsMap[i] = author.ToParams()
	}

	params := map[string]interface{}{
		"authors": authorsMap,
	}

	return params
}

type Article struct {
	Id         string   `json:"_id"`
	Title      string   `json:"title"`
	Authors    []Author `json:"authors"`
	References []string `json:"references"`
}

func (a *Article) ToParams() map[string]interface{} {
	params := map[string]interface{}{
		"_id":   a.Id,
		"title": a.Title,
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
}

var wg sync.WaitGroup

func downloadAndParseJson(url string, articles chan Article, authors chan Author, authoredRels chan AuthoredRel, citesRels chan CitesRel, max int) {
	resp, err := http.Get(url)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	reader := bufio.NewReader(resp.Body)
	cleaner := &NumberIntCleaner{r: reader}

	err = parseJson(cleaner, articles, authors, authoredRels, citesRels, max)
	if err != nil {
		panic(err)
	}
}

/*func readAndParseJson(filepath string, articles chan Article, max int) {
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
}*/

func parseJson(r io.Reader, articles chan Article, authors chan Author, authoredRels chan AuthoredRel, citesRels chan CitesRel, max int) error {
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

		for _, author := range article.Authors {
			authors <- author
			authoredRels <- AuthoredRel{AuthorId: author.Id, ArticleId: article.Id}
		}

		for _, refId := range article.References {
			citesRels <- CitesRel{ArticleId: article.Id, RefId: refId}
		}
	}

	// Last ]
	if _, err := decoder.Token(); err != nil {
		return err
	}

	return nil
}

func connectToDB(dbConf DbConfig) (neo4j.DriverWithContext, neo4j.SessionWithContext, context.Context, error) {
	ctx := context.Background()

	driver, err := neo4j.NewDriverWithContext(
		dbConf.URI,
		neo4j.BasicAuth(dbConf.Username, dbConf.Password, ""),
	)

	if err != nil {
		return nil, nil, nil, err
	}

	session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite, DatabaseName: "neo4j"})

	return driver, session, ctx, nil
}

func pushArticlesToDB(dbConf DbConfig, articles chan Article) {
	query := `
	UNWIND $articles AS article
	MERGE (a:Article {_id: article._id, title: article.title})
	`

	articlesBuf := make([]Article, 0, 50)

	driver, session, ctx, err := connectToDB(dbConf)
	if err != nil {
		panic(err)
	}
	defer driver.Close(ctx)
	defer session.Close(ctx)

	for article := range articles {
		articlesBuf = append(articlesBuf, article)

		if len(articlesBuf) == 50 {
			params := articlesToParams(articlesBuf)
			_, err := session.Run(ctx, query, params)
			if err != nil {
				panic(err)
			}

			articlesBuf = articlesBuf[:0]
		}
	}

	if len(articlesBuf) > 0 {
		params := articlesToParams(articlesBuf)
		_, err := session.Run(ctx, query, params)
		if err != nil {
			panic(err)
		}
	}

	wg.Done()
}

func pushAuthorsToDB(dbConf DbConfig, authors chan Author) {
	query := `
	UNWIND $authors AS author
	MERGE (a:Author {_id: author._id, name: author.name})
	`

	authorsBuf := make([]Author, 0, 50)

	driver, session, ctx, err := connectToDB(dbConf)
	if err != nil {
		panic(err)
	}
	defer driver.Close(ctx)
	defer session.Close(ctx)

	for author := range authors {
		authorsBuf = append(authorsBuf, author)

		if len(authorsBuf) == 50 {
			params := authorsToParams(authorsBuf)
			_, err := session.Run(ctx, query, params)
			if err != nil {
				panic(err)
			}

			authorsBuf = authorsBuf[:0]
		}
	}

	if len(authorsBuf) > 0 {
		params := authorsToParams(authorsBuf)
		_, err := session.Run(ctx, query, params)
		if err != nil {
			panic(err)
		}
	}

	wg.Done()
}

func pushAuthoredRelsToDB(dbConf DbConfig, authoredRels chan AuthoredRel) {
	query := `
	UNWIND $authoredRels AS authoredRel
	MERGE (a:Author {_id: authoredRel.authorId})
	MERGE (b:Article {_id: authoredRel.articleId})
	MERGE (a)-[:AUTHORED]->(b)
	`

	authoredRelsBuf := make([]AuthoredRel, 0, 50)

	driver, session, ctx, err := connectToDB(dbConf)
	if err != nil {
		panic(err)
	}
	defer driver.Close(ctx)
	defer session.Close(ctx)

	for authoredRel := range authoredRels {
		authoredRelsBuf = append(authoredRelsBuf, authoredRel)

		if len(authoredRelsBuf) == 50 {
			params := authoredRelsToParams(authoredRelsBuf)
			_, err := session.Run(ctx, query, params)
			if err != nil {
				panic(err)
			}

			authoredRelsBuf = authoredRelsBuf[:0]
		}
	}

	if len(authoredRelsBuf) > 0 {
		params := authoredRelsToParams(authoredRelsBuf)
		_, err := session.Run(ctx, query, params)
		if err != nil {
			panic(err)
		}
	}

	wg.Done()
}

func pushCitesRelsToDB(dbConf DbConfig, citesRels chan CitesRel) {
	query := `
	UNWIND $citesRels AS citesRel
	MERGE (a:Article {_id: citesRel.articleId})
	MERGE (b:Article {_id: citesRel.refId})
	MERGE (a)-[:CITES]->(b)
	`

	citesRelsBuf := make([]CitesRel, 0, 50)

	driver, session, ctx, err := connectToDB(dbConf)
	if err != nil {
		panic(err)
	}
	defer driver.Close(ctx)
	defer session.Close(ctx)

	for citesRel := range citesRels {
		citesRelsBuf = append(citesRelsBuf, citesRel)

		if len(citesRelsBuf) == 50 {
			params := citesRelsToParams(citesRelsBuf)
			_, err := session.Run(ctx, query, params)
			if err != nil {
				panic(err)
			}

			citesRelsBuf = citesRelsBuf[:0]
		}
	}

	if len(citesRelsBuf) > 0 {
		params := citesRelsToParams(citesRelsBuf)
		_, err := session.Run(ctx, query, params)
		if err != nil {
			panic(err)
		}
	}

	wg.Done()
}

func main() {
	dbConf := DbConfig{
		URI:      "bolt://3.239.216.81:7687",
		Username: "neo4j",
		Password: "horns-mattresses-cashiers",
	}

	articles := make(chan Article, 1000)
	authors := make(chan Author, 1000)
	authoredRels := make(chan AuthoredRel, 1000)
	citesRels := make(chan CitesRel, 1000)

	//url := "http://vmrum.isc.heia-fr.ch/biggertest.json"
	url := "http://vmrum.isc.heia-fr.ch/dblpv13.json"
	//filepath := "data/dblpv13.json"

	wg.Add(3)
	go pushArticlesToDB(dbConf, articles)
	go pushAuthorsToDB(dbConf, authors)
	go pushAuthoredRelsToDB(dbConf, authoredRels)
	//go pushCitesRelsToDB(dbConf, citesRels)

	start := time.Now()

	//go readAndParseJson(filepath, articles, 500)
	go downloadAndParseJson(url, articles, authors, authoredRels, citesRels, 100)
	wg.Wait()

	elapsed := time.Since(start)
	fmt.Printf("Clean + Parse + Add to DB time: %s\n", elapsed)
}
