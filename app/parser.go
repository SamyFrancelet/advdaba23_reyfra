package main

import (
	"bufio"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"regexp"
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

func downloadAndParseJson(url string, articles chan Article, authors chan Author, citesRels chan CitesRel, max int) {
	resp, err := http.Get(url)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	reader := bufio.NewReader(resp.Body)
	cleaner := &NumberIntCleaner{r: reader}

	err = parseJson(cleaner, articles, authors, citesRels, max)
	if err != nil {
		panic(err)
	}
}

func readAndParseJson(filepath string, articles chan Article, authors chan Author, citesRels chan CitesRel, max int) {
	file, err := os.Open(filepath)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	cleaner := &NumberIntCleaner{r: reader}

	err = parseJson(cleaner, articles, authors, citesRels, max)
	if err != nil {
		panic(err)
	}
}

func parseJson(r io.Reader, articles chan Article, authors chan Author, citesRels chan CitesRel, max int) error {
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

		if articles != nil {
			articles <- article
		}

		if authors != nil {
			for _, author := range article.Authors {
				author.ArtId = article.Id
				authors <- author
			}
		}

		if citesRels != nil {
			citesRel := CitesRel{
				ArticleId: article.Id,
				RefIds:    article.References,
			}

			citesRels <- citesRel
		}
	}

	// Last ]
	if _, err := decoder.Token(); err != nil {
		return err
	}

	if articles != nil {
		close(articles)
	}

	if authors != nil {
		close(authors)
	}

	if citesRels != nil {
		close(citesRels)
	}

	return nil
}
