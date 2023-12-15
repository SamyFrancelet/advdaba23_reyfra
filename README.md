# Large database experiment with Neo4j
Repository for the second neo4j lab of the AdvDaBa course @ MSE

## Useful links
- [Neo4j](https://neo4j.com/)
- [Dataset source](https://www.aminer.org/citation)
- [Dataset zip](https://originalstatic.aminer.cn/misc/dblp.v13.7z)
- [Small Test DB](http://vmrum.isc.heia-fr.ch/biggertest.json)
- [Scaled Test DB](http://vmrum.isc.heia-fr.ch/dblpv13.json)

## Run the project
```bash
./build.sh
docker-compose up -d
```

## Description of the approach
### Data parsing
The data is downloaded and parsed in a streaming fashion. Because the file isn't a standard JSON, 
a custom reader is used to clean the file. It buffers some lines and then uses a regex to
remove the "NumberInt()" around the numbers.
This cleaner is just another reader added in the chain http.Get -> json.Decoder, giving a new
http.Get -> customReader -> json.Decoder chain.

### Data insertion to Neo4j
The data is inserted in Neo4j using the official Go driver.
The json parser will push data to given channels, and custom goroutines
will read from these channels and insert the data in Neo4j.
To reduce memory usage, and to avoid possible deadlocks,
only one goroutine will insert data for a given type of node or relationship.

### Queries
The first goroutine will insert only the Article Nodes:

```
UNWIND $articles AS article
CREATE (a:Article {_id: article._id, title: article.title})
```
with articles being a large number of Article objects.
Because the database is empty, we can simply CREATE the nodes.
After the first batch of articles, a custom index is created
on the _id property of the Article nodes:

```
CREATE INDEX article_id IF NOT EXISTS FOR (a:Article) ON (a._id)
```
Because every other query will search for an article by its _id,
this index will speed up significantly the queries.

The second goroutine will insert the Authors nodes and AUTHORED relationships:

```
UNWIND $authors AS author
CREATE (a:Author {_id: author._id, name: author.name})
MERGE (b:Article {_id: author.art_id})
MERGE (a)-[:AUTHORED]->(b)
```

Then the last goroutine will insert the Citation relationships:

```
UNWIND $citesRels AS citesRel
MATCH (a:Article {_id: citesRel.articleId})
WITH a, citesRel
UNWIND citesRel.refIds AS refId
MERGE (b:Article {_id: refId})
MERGE (a)-[:CITES]->(b)
```

## Results
Unfortunately, running this experiment on a rancher pod wasn't possible
due to unsolved issues with the network. The database was running on the
same pod as the application, and the application couldn't resolve the
database hostname.

However, running the experiment locally gave the following results:


## Improvements
- Instead of waiting 200s for the database to be ready, we could use a retry mechanism