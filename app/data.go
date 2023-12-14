package main

type DbConfig struct {
	URI      string
	Username string
	Password string
}

type CitesRel struct {
	ArticleId string
	RefIds    []string
}

func (cr *CitesRel) ToParams() map[string]interface{} {
	params := map[string]interface{}{
		"articleId": cr.ArticleId,
		"refIds":    cr.RefIds,
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
	Id    string `json:"_id"`
	Name  string `json:"name"`
	ArtId string
}

func (a *Author) ToParams() map[string]interface{} {
	params := map[string]interface{}{
		"_id":    a.Id,
		"name":   a.Name,
		"art_id": a.ArtId,
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
		//"refs":  a.References,
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
