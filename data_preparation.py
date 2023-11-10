from datetime import datetime
import json
import math
import os
import pandas as pd
import pathlib
import sys


def read_csv(path, names=None, delimiter='\t'):
    return pd.read_csv(
        path,
        delimiter=delimiter,
        quoting=3,
        na_values='\\N',
        header=0,
        names=names,
    )


def isna(x):
    if isinstance(x, float) and math.isnan(x):
        return True
    if isinstance(x, str) and x == "":
        return True
    if isinstance(x, list) and x == []:
        return True
    if isinstance(x, dict) and x == {}:
        return True
    return False


def dropna(d):
    if not isinstance(d, dict):
        return d

    rd = d.copy()
    for k in d:
        if isinstance(d[k], dict):
            rd[k] = dropna(d[k])
        elif isinstance(d[k], list):
            rd[k] = [dropna(x) for x in d[k]]
        elif isna(d[k]):
            del rd[k]
    return rd


# https://stackoverflow.com/a/52137753: dumping large json arrays
class IteratorAsList(list):
    def __init__(self, iterator):
        self.iterator = iterator

    def __iter__(self):
        return self.iterator

    def __len__(self):
        return 1


def people():
    infile = pathlib.PurePath("data", "imdb", "name.basics.tsv")
    df = read_csv(infile, ["id", "name", "birth", "death", "professions", "knownFor"])

    df.knownFor = df.knownFor.fillna("").map(lambda x: x.split(','))
    df.professions = df.professions.fillna("").map(lambda x: x.split(','))

    outfile = pathlib.PurePath("collections", "people.json")
    with open(outfile, 'w') as f:
        json.dump(
            IteratorAsList(dropna(record) for record in df.to_dict("records")),
            f,
            ensure_ascii=False
        )


def shows():
    def load_plots():
        filepath = pathlib.PurePath("data", "misc", "plots.csv")
        plots = {}
        with open(filepath, 'r') as f:

            for line in f:
                tid, plot = line.strip().split(',', 1)
                plots[tid] = plot
        return plots

    def load_principals():
        filepath = pathlib.PurePath("data", "imdb", "title.principals")
        df = read_csv(filepath, ["tid", "ordering", "pid", "category", "job", "characters"])

        def process_characters(s):
            if isinstance(s, float) and math.isnan(s):
                return ""
            assert isinstance(s, str)
            return [char.strip() for char in s[1:-1].replace('"', '').split(',')]

        df["characters"] = df["characters"].apply(process_characters)

        # Needlessly gendered
        df["category"] = df.category.replace("actress", "actor")
        return df

    def load_ratings():
        filepath = pathlib.PurePath("data", "imdb", "title.ratings")
        return read_csv(filepath, ["tid", "averageRating", "numVotes"])

    def load_basics():
        filepath = pathlib.PurePath("data", "imdb", "title.basics")
        df = read_csv(filepath,
                      ["tid", "ttype", "title", "original", "isAdult", "start", "end", "runtime", "genres"])

        df.drop(["original"], axis=1, inplace=True)

        df.fillna({
            "isAdult": 2,
            "genres": "",
        }, inplace=True)

        df["isAdult"] = df["isAdult"].map({0: "no", 1: "yes", 2: ""})

        return df

    def load_akas():
        filepath = pathlib.PurePath("data", "imdb", "title.akas")
        df = read_csv(filepath, ["tid", "ordering", "title", "A", "B", "C", "D", "E"])
        return df[["tid", "ordering", "title"]]

    def load_tags():
        filepath = pathlib.PurePath("data", "ml-25m", "links.csv")
        df_links = read_csv(filepath, delimiter=',')

        filepath = pathlib.PurePath("data", "ml-25m", "genome-tags.csv")
        df_tagnames = read_csv(filepath, delimiter=',')

        filepath = pathlib.PurePath("data", "ml-25m", "genome-scores.csv")
        df_tags = read_csv(filepath, delimiter=',')

        df_tags = df_tags.join(df_tagnames.set_index("tagId"), on="tagId")
        df_tags = df_tags.join(df_links.set_index("movieId"), on="movieId")
        return df_tags.drop(["tagId", "movieId", "tmdbId"], axis=1)

    def get_people(tid):
        actors = df_principals[(df_principals.tid == tid) & (df_principals.category == "actor")].sort_values("ordering")
        character_list = [
            {
                "id": d["pid"],
                "characters": list(d["characters"]),
            }
            for d in actors.to_dict("records")
        ]

        crew = df_principals[(df_principals.tid == tid) & (df_principals.category != "actor")].sort_values("ordering")
        job_list = [
            {
                "id": d["pid"],
                "category": d["category"],
                "job": d["job"] if d["job"] != d["category"] else "",
            }
            for d in crew.to_dict("records")
        ]

        return {
            "actors": character_list,
            "crew": job_list,
        }

    def get_basics(tid):
        basic = df_basics[df_basics.tid == tid].iloc[0]
        akas = df_akas[df_akas.tid == tid].sort_values("ordering").title.tolist()
        return {
            "title": basic.title,
            "type": basic.ttype,
            "start": basic.start,
            "end": basic.end,
            "runtime": basic.runtime,
            "isAdult": basic.isAdult,
            "genres": basic.genres.split(',') if basic.genres else [],
            "aka": list(set(akas)),
        }

    def get_ratings(tid):
        query = df_ratings[df_ratings.tid == tid]
        if len(query) == 0:
            return {}

        rating = query.iloc[0]
        return {
            "rating": float(rating.averageRating),
            "votes": int(rating.numVotes),
        }

    def get_meta(tid):
        query = df_tags[df_tags.imdbId == int(tid[2:])]
        if len(query) == 0:
            return {}

        # 20 tags sufficient, there are like 1k total
        tags = query.sort_values("relevance", ascending=False).head(20)
        return {
            "tags": [
                {"tag": tag, "relevance": relevance}
                for tag, relevance in zip(tags.tag, tags.relevance)
            ],
            "plot": plots.get(tid, ""),
        }

    plots = load_plots()

    df_akas = load_akas()
    df_basics = load_basics()
    df_principals = load_principals()
    df_ratings = load_ratings()
    df_tags = load_tags()

    tids = list(set(df_basics.tid))
    outfile = pathlib.PurePath("collections", "shows.json")
    with open(outfile, 'w') as f:
        json.dump([
            dropna({
                "_id": tid,
                "tid": tid,
                "basics": get_basics(tid),
                "people": get_people(tid),
                "rating": get_ratings(tid),
                "meta": get_meta(tid),
            }) for tid in tids
        ], f, ensure_ascii=False, indent=4)


def user_ratings():
    filepath = pathlib.PurePath("data", "ml-25m", "links.csv")
    df = pd.read_csv(filepath, dtype={"movieId": int, "imdbId": str})
    df = df.drop(["tmdbId"], axis=1)
    movieId_to_imdbId = df.set_index("movieId").imdbId.map(lambda s: "tt" + s).to_dict()

    filepath = pathlib.PurePath("data", "ml-25m", "ratings.csv")
    df = pd.read_csv(filepath)
    df.movieId = df.movieId.map(lambda x: movieId_to_imdbId[x])

    df.rename(columns={"movieId": "imdbId"}, inplace=True)

    outfile = pathlib.PurePath("collections", "user_ratings.csv")
    df.to_csv(outfile, index=False)


def user_events():
    filepath = pathlib.PurePath("data", "misc", "user_events.csv")
    df = pd.read_csv(filepath)

    df = df.drop(["tag", "rating"], axis=1)
    df.eventType = df.eventType.map(lambda x: x.split('-')[1])

    outfile = pathlib.PurePath("collections", "user_events.csv")
    df.to_csv(outfile, index=False)


def prepare_collections(collections):
    os.makedirs("collections", exist_ok=True)

    if "all" in collections:
        collections = known_collections[1:]

    for collection in collections:
        if collection == "people":
            people()
        elif collection == "shows":
            shows()
        elif collection == "user_ratings":
            user_ratings()
        elif collection == "user_events":
            user_events()
        else:
            continue


if __name__ == "__main__":
    known_collections = ["all", "people", "shows", "user_ratings", "user_events"]

    if len(sys.argv) == 1:
        print(f"Usage: python {sys.argv[0]} <list of collections>")
        print(f"Collections: {known_collections}")
        sys.exit(1)

    prepare_collections(sys.argv[1:])
