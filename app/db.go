package main

import (
	"context"

	"github.com/fatih/color"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

const MaxDataPerQuery = 16384

//const MaxDataPerQuery = 1000

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
	CREATE (a:Article {_id: article._id, title: article.title})
	`

	articlesBuf := make([]Article, 0, MaxDataPerQuery)

	driver, session, ctx, err := connectToDB(dbConf)
	if err != nil {
		panic(err)
	}
	defer driver.Close(ctx)
	defer session.Close(ctx)

	i := 0
	indexed := false

	for article := range articles {
		articlesBuf = append(articlesBuf, article)
		i++

		if len(articlesBuf) == MaxDataPerQuery {
			params := articlesToParams(articlesBuf)

			color.Blue("Articles\t pushing:\t%d", i)

			_, err := session.Run(ctx, query, params)
			if err != nil {
				panic(err)
			}

			articlesBuf = articlesBuf[:0]

			color.Blue("Articles\t pushed:\t%d", i)

			// Only after the first batch of articles,
			// we can create an Index on the _id property
			// of the Article nodes.
			if !indexed {
				_, err := session.Run(ctx, "CREATE INDEX article_id IF NOT EXISTS FOR (a:Article) ON (a._id)", nil)
				if err != nil {
					panic(err)
				}

				color.Blue("Articles\t index created!")

				indexed = true
			}
		}
	}

	if len(articlesBuf) > 0 {
		params := articlesToParams(articlesBuf)
		_, err := session.Run(ctx, query, params)
		if err != nil {
			panic(err)
		}
	}

	color.Blue("Total articles pushed: %d", i)

	wg.Done()
}

func pushAuthorsToDB(dbConf DbConfig, authors chan Author) {
	query := `
	UNWIND $authors AS author
	CREATE (a:Author {_id: author._id, name: author.name})
	MERGE (b:Article {_id: author.art_id})
	MERGE (a)-[:AUTHORED]->(b)
	`

	authorsBuf := make([]Author, 0, MaxDataPerQuery)

	driver, session, ctx, err := connectToDB(dbConf)
	if err != nil {
		panic(err)
	}
	defer driver.Close(ctx)
	defer session.Close(ctx)

	i := 0

	for author := range authors {
		authorsBuf = append(authorsBuf, author)
		i++

		if len(authorsBuf) == MaxDataPerQuery {
			params := authorsToParams(authorsBuf)

			color.Green("Authors\t\t pushing:\t%d", i)

			_, err := session.Run(ctx, query, params)
			if err != nil {
				panic(err)
			}

			authorsBuf = authorsBuf[:0]

			color.Green("Authors\t\t pushed:\t%d", i)
		}
	}

	if len(authorsBuf) > 0 {
		params := authorsToParams(authorsBuf)
		_, err := session.Run(ctx, query, params)
		if err != nil {
			panic(err)
		}
	}

	color.Green("Total authors pushed: %d", i)

	wg.Done()
}

func pushCitesRelsToDB(dbConf DbConfig, citesRels chan CitesRel) {
	query := `
	UNWIND $citesRels AS citesRel
	MATCH (a:Article {_id: citesRel.articleId})
	WITH a, citesRel
	UNWIND citesRel.refIds AS refId
	MERGE (b:Article {_id: refId})
	MERGE (a)-[:CITES]->(b)
	`

	citesRelsBuf := make([]CitesRel, 0, MaxDataPerQuery)

	driver, session, ctx, err := connectToDB(dbConf)
	if err != nil {
		panic(err)
	}
	defer driver.Close(ctx)
	defer session.Close(ctx)

	i := 0

	for citesRel := range citesRels {
		citesRelsBuf = append(citesRelsBuf, citesRel)
		i++

		if len(citesRelsBuf) == MaxDataPerQuery {
			params := citesRelsToParams(citesRelsBuf)

			color.Magenta("CitesRels\t pushing:\t%d", i)

			_, err := session.Run(ctx, query, params)
			if err != nil {
				panic(err)
			}

			citesRelsBuf = citesRelsBuf[:0]

			color.Magenta("CitesRels\t pushed:\t%d", i)
		}
	}

	if len(citesRelsBuf) > 0 {
		params := citesRelsToParams(citesRelsBuf)
		_, err := session.Run(ctx, query, params)
		if err != nil {
			panic(err)
		}
	}

	color.Magenta("Total citesRels pushed: %d", i)

	wg.Done()
}
