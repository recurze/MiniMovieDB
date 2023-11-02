## Mini Movie DB


## Datasets

Use `data_collection.sh` to download and extract into following directory structure.

```
data
|-- imdb/*.tsv
|-- ml-25/*.csv
\-- misc/*.csv
```

* [IMDb non-commericial datasets](https://developer.imdb.com/non-commercial-datasets/)
* [MovieLens](https://grouplens.org/datasets/movielens/25m/)
* [Movie Genres](https://www.davidsbatista.net/blog/2017/04/01/document_classification/)


## Cleanup and restructure

Use `data_preparation.py` to convert tsv/csv into json for MongoDB import. See `collections.txt` for schema.


## `mongoimport`
