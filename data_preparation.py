from datetime import datetime
import json
import math
import os
import pandas as pd
import pathlib
import sys
import time


def get_oid(s):
    if not isinstance(s, str):
        s = str(s)
    hex = s.encode('utf-8').hex()
    return {"$oid": '0'*(24 - len(hex)) + hex if hex else ""}


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

    def get_known_for(kf):
        if isinstance(kf, str):
            return kf.split(',')
        return []

    def people_dict(record):
        return dropna({
            "_id": get_oid(record["id"]),
            "name": record["name"],
            "birth": record["birth"],
            "death": record["death"],
            "professions": record["professions"],
            "knownFor": [get_oid(id) for id in get_known_for(record["knownFor"])],
        })

    outfile = pathlib.PurePath("collections", "people.json")
    with open(outfile, 'w') as f:
        json.dump(
            IteratorAsList(people_dict(record) for record in df.to_dict("records")),
            f,
            ensure_ascii=False,
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
        filepath = pathlib.PurePath("data", "imdb", "title.principals.tsv")
        df = read_csv(filepath, ["tid", "ordering", "pid", "category", "job", "characters"])

        # Needlessly gendered
        df["category"] = df.category.replace("actress", "actor")
        return df.sort_values("ordering")

    def load_ratings():
        filepath = pathlib.PurePath("data", "imdb", "title.ratings.tsv")
        return read_csv(filepath, ["tid", "averageRating", "numVotes"])

    def load_basics():
        filepath = pathlib.PurePath("data", "imdb", "title.basics.tsv")
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
        filepath = pathlib.PurePath("data", "imdb", "title.akas.tsv")
        df = read_csv(filepath, ["tid", "ordering", "title", "A", "B", "C", "D", "E"])
        return df[["tid", "ordering", "title"]].sort_values("ordering")

    def load_tags():
        filepath = pathlib.PurePath("data", "ml-25m", "links.csv")
        df_links = read_csv(filepath, delimiter=',')

        filepath = pathlib.PurePath("data", "ml-25m", "genome-tags.csv")
        df_tagnames = read_csv(filepath, delimiter=',')

        filepath = pathlib.PurePath("data", "ml-25m", "genome-scores.csv")
        df_tags = read_csv(filepath, delimiter=',')

        df_tags = df_tags.join(df_tagnames.set_index("tagId"), on="tagId")
        df_tags = df_tags.join(df_links.set_index("movieId"), on="movieId")
        return df_tags.drop(["tagId", "movieId", "tmdbId"], axis=1).sort_values("relevance", ascending=False)

    def process_characters(s):
        if isinstance(s, float) and math.isnan(s):
            return ""
        assert isinstance(s, str)
        return [char.strip() for char in s[1:-1].replace('"', '').split(',')]

    def get_people(tid):
        actors = df_principals[(df_principals.tid == tid) & (df_principals.category == "actor")]
        character_list = [
            {
                "id": get_oid(d["pid"]),
                "characters": process_characters(d["characters"]),
            }
            for d in actors.to_dict("records")
        ]

        crew = df_principals[(df_principals.tid == tid) & (df_principals.category != "actor")]
        job_list = [
            {
                "id": get_oid(d["pid"]),
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
        akas = df_akas[df_akas.tid == tid].title.tolist()
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
        tags = query.head(20)
        return {
            "tags": [
                {"tag": tag, "relevance": relevance}
                for tag, relevance in zip(tags.tag, tags.relevance)
            ],
            "plot": plots.get(tid, ""),
        }

    def show_dict(tid):
        return dropna({
            "_id": get_oid(tid),
            "tid": tid,
            "basics": get_basics(tid),
            "people": get_people(tid),
            "rating": get_ratings(tid),
            "meta": get_meta(tid),
        })

    plots = load_plots()

    df_akas = load_akas()
    df_basics = load_basics()
    df_principals = load_principals()
    df_ratings = load_ratings()
    df_tags = load_tags()

    tids = list(set(df_basics.tid))
    outfile = pathlib.PurePath("collections", "shows.json")
    with open(outfile, 'w') as f:
        json.dump(
            IteratorAsList(show_dict(tid) for tid in tids),
            f,
            ensure_ascii=False,
        )


def users():
    def load_ratings():
        filepath = pathlib.PurePath("data", "ml-25m", "links.csv")
        df_links = read_csv(filepath, delimiter=',')

        filepath = pathlib.PurePath("data", "ml-25m", "ratings.csv")
        df = read_csv(filepath, delimiter=',')

        df = df.join(df_links.set_index("movieId"), on="movieId")
        return df.drop(["tmdbId"], axis=1)

    def load_events():
        filepath = pathlib.PurePath("data", "misc", "user_events.csv")
        df_events = pd.read_csv(filepath, delimiter=',', quotechar='"')
        df_events.drop(["tag"], axis=1, inplace=True)
        return df_events

    def make_imdb_id(s):
        if not isinstance(s, str):
            if math.isnan(s):
                return ''
            s = str(int(s))
        return s if len(s) >= 7 else "tt" + '0'*(7 - len(s)) + s

    def make_timestamp(t):
        if math.isnan(t):
            return ""
        return datetime.fromtimestamp(t).strftime('%Y%m%d %H:%M:%S')

    def get_ratings(uid):
        return [
            {
                "movieId": d["movieId"],
                "rating": d["rating"],
                "imdbId": get_oid(make_imdb_id(d["imdbId"])),
                "timestamp": make_timestamp(d["timestamp"]),
            }
            for d in df_ratings[df_ratings.userId == uid].to_dict("records")
        ]

    def get_events(uid):
        return [
            {
                "sessionId": d["sessionId"],
                "eventType": d["eventType"].split('-')[1],
                "imdbId": get_oid(make_imdb_id(d["imdbId"])),
                "timestamp": make_timestamp(d["timestamp"]),
                # Only available for eventType = playback
                "timestamp_end": make_timestamp(d["playbackEndTimestamp"]),
                # Only available for eventType = rate
                "rating": d["rating"],
            }
            for d in df_events[df_events.userId == uid].sort_values("timestamp").to_dict("records")
        ]

    def user_dict(uid):
        return dropna({
            "_id": get_oid(uid),
            "ratings": get_ratings(uid),
            "events": get_events(uid),
        })

    df_ratings = load_ratings()
    df_events = load_events()

    uids = list(set(df_ratings.userId))
    outfile = pathlib.PurePath("collections", "users.json")
    with open(outfile, 'w') as f:
        json.dump(
            IteratorAsList(user_dict(uid) for uid in uids),
            f,
            ensure_ascii=False,
        )


def prepare_collections(collections):
    os.makedirs("collections", exist_ok=True)

    if "all" in collections:
        collections = known_collections[1:]

    for collection in collections:
        start = time.time()
        print(f"Preparing {collection}")
        if collection == "people":
            people()
        elif collection == "shows":
            shows()
        elif collection == "users":
            users()
        else:
            continue
        print(f"Time taken: {time.time() - start}")


if __name__ == "__main__":
    known_collections = ["all", "people", "shows", "users"]

    if len(sys.argv) == 1:
        print(f"Usage: python {sys.argv[0]} <list of collections>")
        print(f"Collections: {known_collections}")
        sys.exit(1)

    prepare_collections(sys.argv[1:])
