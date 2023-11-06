from imdb import Cinemagoer
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


def people():
    infile = pathlib.PurePath("data", "imdb", "name.basics")
    df = read_csv(infile, ["id", "name", "birth", "death", "professions", "knownFor"])

    outfile = pathlib.PurePath("collections", "people.json")

    with open(outfile, 'w') as f:
        json.dump([dropna(record) for record in df.to_dict("records")], f, ensure_ascii=False)


def shows():
    def load_actors():
        filepath = pathlib.PurePath("data", "imdb", "title.principals")
        df = read_csv(filepath, ["tid", "ordering", "pid", "category", "job", "character"])

        def process_characters(s):
            if isinstance(s, float) and math.isnan(s):
                return ""
            assert isinstance(s, str)
            return [char.strip() for char in s[1:-1].replace('"', '').split(',')]

        df["character"] = df["character"].apply(process_characters)

        # Needlessly gendered
        df["category"] = df.category.replace("actress", "actor")

        # I'm sorry everyone else
        return df[df.category == "actor"]

    def load_crew():
        filepath = pathlib.PurePath("data", "imdb", "title.crew")
        return read_csv(filepath, ["tid", "directors", "writers"]).fillna("")

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
        actors = df_actors[df_actors.tid == tid].sort_values("ordering")
        character_list = [
            {"id": pid, "characters": character}
            for pid, character in zip(actors.pid, list(actors.character))
        ]

        query = df_crew[df_crew.tid == tid]
        if len(query) == 0:
            return {
                "actors": character_list,
            }

        directors_writers = query.iloc[0]
        return {
            "directors": directors_writers.directors.split(',') if directors_writers.directors else [],
            "writers": directors_writers.writers.split(',') if directors_writers.writers else [],
            "actors": character_list,
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
        #plot = cgo.get_movie(tid[2:], info=["plot"]).get("plot", " ")[0]
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
            #"plot": plot,
        }

    #cgo = Cinemagoer()

    df_actors = load_actors()
    df_akas = load_akas()
    df_basics = load_basics()
    df_crew = load_crew()
    df_ratings = load_ratings()
    df_tags = load_tags()

    tids = list(set(df_basics.tid))
    outfile = pathlib.PurePath("collections", "shows.json")
    with open(outfile, 'w') as f:
        json.dump([
            dropna({
                "id": tid,
                "basics": get_basics(tid),
                "people": get_people(tid),
                "rating": get_ratings(tid),
                "meta": get_meta(tid),
            }) for tid in tids
        ], f, ensure_ascii=False, indent=4)


def users():
    def load_ratings():
        filepath = pathlib.PurePath("data", "ml-25m", "links")
        df_links = read_csv(filepath, delimiter=',')

        filepath = pathlib.PurePath("data", "ml-25m", "ratings")
        df = read_csv(filepath, delimiter=',')

        df = df.join(df_links.set_index("movieId"), on="movieId")
        return df.drop(["tmdbId", "timestamp"], axis=1)

    def get_ratings(uid):
        def make_imdb_id(s):
            if not isinstance(s, str):
                if math.isnan(s):
                    return ''
                s = str(int(s))

            return s if len(s) >= 7 else "tt" + '0'*(7 - len(s)) + s

        return [
            {
                "movieId": d["movieId"],
                "rating": d["rating"],
                "imdbId": make_imdb_id(d["imdbId"]),
            }
            for d in df_ratings[df_ratings.userId == uid].to_dict("records")
        ]

    df_ratings = load_ratings()
    uids = list(set(df_ratings.userId))

    outfile = pathlib.PurePath("collections", "users.json")
    with open(outfile, 'w') as f:
        json.dump([
            dropna({
                "id": uid,
                "ratings": get_ratings(uid),
            }) for uid in uids
        ], f, ensure_ascii=False, indent=4)


def prepare_collections(collections):
    os.makedirs("collections", exist_ok=True)

    if "all" in collections:
        collections = known_collections[1:]

    for collection in collections:
        if collection == "people":
            people()
        elif collection == "shows":
            shows()
        elif collection == "users":
            users()
        else:
            continue


if __name__ == "__main__":
    known_collections = ["all", "people", "shows", "users"]

    if len(sys.argv) == 1:
        print(f"Usage: python {sys.argv[0]} <list of collections>")
        print(f"Collections: {known_collections}")
        sys.exit(1)

    prepare_collections(sys.argv[1:])
