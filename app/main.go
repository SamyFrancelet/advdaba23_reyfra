package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"
)

type NumberIntCleaner struct {
	//r io.Reader
	r      *bufio.Reader
	buffer string
}

func (nic *NumberIntCleaner) Read(p []byte) (n int, err error) {
	/*for {
		line, err := nic.r.ReadString('\n')
		if err != nil && err != io.EOF {
			return 0, err
		}

		nic.buffer += line

		// Search for a complete NumberInt()
		re := regexp.MustCompile(`NumberInt\(([^)]+)\)`)
		cleaned := re.ReplaceAllString(nic.buffer, "$1")

		if len(cleaned) > len(p) {
			n = copy(p, cleaned)
			nic.buffer = cleaned[n:]
			return n, nil
		}

		nic.buffer = cleaned

		if err == io.EOF {
			return copy(p, cleaned), err
		}
	}*/

	buf := make([]byte, len(p))
	n, err = nic.r.Read(buf)
	if err != nil && err != io.EOF {
		return 0, err
	}

	// Search for a complete NumberInt()
	re := regexp.MustCompile(`NumberInt\(([^)]+)\)`)
	cleaned := re.ReplaceAllString(string(buf[:n]), "$1")

	return copy(p, cleaned), err
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
	NCitations int      `json:"n_citation"`
	References []string `json:"references"`

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

func downloadJson(url string) error {
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	reader := bufio.NewReader(resp.Body)
	cleaner := &NumberIntCleaner{r: reader}

	decoder := json.NewDecoder(cleaner)

	// First [
	if _, err := decoder.Token(); err != nil {
		return err
	}

	i := 0

	for decoder.More() {
		var art Article

		// Start timing
		//start := time.Now()
		if err := decoder.Decode(&art); err != nil {
			return err
		}
		/*fmt.Printf("Title: %s\nAuthors: \n", art.Title)
		for _, author := range art.Authors {
			fmt.Printf("\t- %s\n", author.Name)
		}*/

		// End timing
		//elapsed := time.Since(start)
		//fmt.Printf("%d: %s\n", i, elapsed)
		if i%100 == 0 {
			fmt.Println(i)
		}
		i++
	}

	// Last ]
	if _, err := decoder.Token(); err != nil {
		return err
	}

	return nil
}

func parseJson(filepath string) error {
	file, err := os.Open(filepath)
	if err != nil {
		return err
	}

	reader := bufio.NewReader(file)
	cleaner := &NumberIntCleaner{r: reader}

	decoder := json.NewDecoder(cleaner)

	// First [
	if _, err := decoder.Token(); err != nil {
		return err
	}

	i := 0

	for decoder.More() {
		var art Article

		// Start timing
		start := time.Now()
		if err := decoder.Decode(&art); err != nil {
			return err
		}
		fmt.Printf("Title: %s\nAuthors: \n", art.Title)
		for _, author := range art.Authors {
			fmt.Printf("\t- %s\n", author.Name)
		}

		// End timing
		elapsed := time.Since(start)
		fmt.Printf("%d: %s\n", i, elapsed)
		i++
	}

	// Last ]
	if _, err := decoder.Token(); err != nil {
		return err
	}

	return nil
}

func downloadFile(url string) error {
	out, err := os.Create(strings.Split(url, "/")[len(strings.Split(url, "/"))-1])
	if err != nil {
		return err
	}
	defer out.Close()

	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	reader := bufio.NewReader(resp.Body)
	cleaner := &NumberIntCleaner{r: reader}

	_, err = io.Copy(out, cleaner)
	if err != nil {
		return err
	}

	return nil
}

func main() {
	//url := "http://vmrum.isc.heia-fr.ch/biggertest.json"
	//url := "http://vmrum.isc.heia-fr.ch/dblpv13.json"
	//filepath := "data/dblpv13.json"
	filepath := "data/dblpv13_corr.json"

	start := time.Now()
	//err := downloadJson(url)
	//err := downloadFile(url)
	err := parseJson(filepath)
	if err != nil {
		fmt.Println("Error:", err)
	}
	elapsed := time.Since(start)

	fmt.Printf("Total time: %s\n", elapsed)
}
