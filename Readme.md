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
* plots.csv generated using [cinemagoer](https://cinemagoer.github.io/).


## Cleanup and restructure

Use `data_preparation.py` to process tsv/csv into csv/json for MongoDB import. See `collections.txt` for schema. Takes around 30 minutes.


## `mongoimport`

The previous step will create files: `collections/$collection.{csv,json}`. Run:

```
sh import_collection_from_file.sh collections/people.json --drop
sh import_collection_from_file.sh collections/user_events.csv --drop
sh import_collection_from_file.sh collections/user_ratings.csv --drop
sh import_collection_from_file.sh collections/shows.csv --drop
sh import_collection_from_file.sh collections/shows.json --mode=merge
sh import_collection_from_file.sh collections/shows.actors.json --mode=merge
sh import_collection_from_file.sh collections/shows.crew.json --mode=merge
```

Takes around 30 minutes
