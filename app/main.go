package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"time"
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

func downloadAndParseJson(url string, nArticles int) error {
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	reader := bufio.NewReader(resp.Body)
	cleaner := &NumberIntCleaner{r: reader}

	return parseJson(cleaner, nArticles)
}

func readAndParseJson(filepath string, nArticles int) error {
	file, err := os.Open(filepath)
	if err != nil {
		return err
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	cleaner := &NumberIntCleaner{r: reader}

	return parseJson(cleaner, nArticles)
}

func parseJson(r io.Reader, nArticles int) error {
	decoder := json.NewDecoder(r)

	// First [
	if _, err := decoder.Token(); err != nil {
		return err
	}

	i := 0

	for decoder.More() && i < nArticles {
		var art Article

		if err := decoder.Decode(&art); err != nil {
			return err
		}

		i++
	}

	// Last ]
	if _, err := decoder.Token(); err != nil {
		return err
	}

	return nil
}

func main() {
	//url := "http://vmrum.isc.heia-fr.ch/biggertest.json"
	//url := "http://vmrum.isc.heia-fr.ch/dblpv13.json"
	filepath := "data/dblpv13.json"
	//filepath := "data/dblpv13_corr.json"
	//filepath := "data/dblpv13_cleaned.json"

	start := time.Now()
	//err := downloadAndParseJson(url)
	err := readAndParseJson(filepath, 1000000)
	if err != nil {
		fmt.Println("Error:", err)
	}
	elapsed := time.Since(start)

	fmt.Printf("Total time: %s\n", elapsed)
}
