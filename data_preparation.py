from imdb import Cinemagoer
import os
import json
import sys
import numpy as np
import pandas as pd
import pathlib


def read_csv(path, names):
    return pd.read_csv(
        path,
        delimiter='\t',
        quoting=3,
        na_values='\\N',
        header=0,
        names=names,
    )


def people():
    infile = pathlib.PurePath("data", "imdb", "name.basics")
    df = read_csv(infile, ["id", "name", "birth", "death", "professions", "knownFor"])

    df[["birth", "death"]] = df[["birth", "death"]].fillna(9999).astype(int)

    outfile = pathlib.PurePath("collections", "people.json")
    df.to_json(outfile, orient="records")


def shows():
    def load_actors():
        filepath = pathlib.PurePath("data", "imdb", "title.principals")
        df = read_csv(filepath, ["tid", "ordering", "pid", "category", "job", "character"])

        # Needlessly gendered
        df["category"] = df.category.replace("actress", "actor")

        # I'm sorry everyone else
        return df[df.category == "actor"]

    def load_crew():
        filepath = pathlib.PurePath("data", "imdb", "title.crew")
        return read_csv(filepath, ["tid", "directors", "writers"]).fillna("")

    def load_basics():
        filepath = pathlib.PurePath("data", "imdb", "title.basics")
        df = read_csv(filepath,
                      ["tid", "ttype", "title", "original", "isAdult", "start", "end", "runtime", "genres"])

        df.drop(["ttype", "original"], axis=1, inplace=True)

        df.fillna({
            "runtime": -1,
            "start": 9999,
            "isAdult": 0,
            "genres": "",
        }, inplace=True)

        df["end"] = np.where(df.end.isna(), df.start, df.end)
        df["isAdult"] = df["isAdult"].map({0: "no", 1: "yes"})

        return df

    def load_akas():
        filepath = pathlib.PurePath("data", "imdb", "title.akas")
        df = read_csv(filepath, ["tid", "ordering", "title", "A", "B", "C", "D", "E"])
        return df[["tid", "ordering", "title"]]

    def get_people(tid):
        actors = df_actors[df_actors.tid == tid].sort_values("ordering")
        directors_writers = df_crew[df_crew.tid == tid].iloc[0]
        return {
            "directors": directors_writers.directors.split(',') if directors_writers.directors else [],
            "writers": directors_writers.writers.split(',') if directors_writers.writers else [],
            "actors": [*zip(actors.pid, actors.character)],
        }

    def get_basics(tid):
        basic = df_basics[df_basics.tid == tid].iloc[0]
        akas = df_akas[df_akas.tid == tid].sort_values("ordering").title.tolist()
        return {
            "title": basic.title,
            "start": int(basic.start),
            "end": int(basic.end),
            "runtime": int(basic.runtime),
            "isAdult": basic.isAdult,
            "genres": basic.genres.split(',') if basic.genres else [],
            "aka": list(set(akas)),
        }

    def get_meta(tid):
        plot = cgo.get_movie(tid[2:], info=["plot"]).get("plot", " ")[0]
        return {
            "tags": '',
            "plot": plot,
        }

    #cgo = Cinemagoer()

    df_actors, df_crew = load_actors(), load_crew()
    df_basics, df_akas = load_basics(), load_akas()
    tids = list(set(df_basics.tid))

    outfile = pathlib.PurePath("collections", "shows.json")
    with open(outfile, 'w') as f:
        json.dump({
            tid: {
                "basics": get_basics(tid),
                "people": get_people(tid),
                #"meta": get_meta(tid),
            } for tid in tids
        }, f, ensure_ascii=False)


def prepare_collections(collections):
    os.makedirs("collections", exist_ok=True)

    if "all" in collections:
        collections = known_collections[1:]

    for collection in collections:
        if collection == "people":
            people()
        elif collection == "shows":
            shows()
        else:
            continue


if __name__ == "__main__":
    known_collections = ["all", "episodes", "people", "shows", "users"]

    if len(sys.argv) == 1:
        print(f"Usage: python {sys.argv[0]} <list of collections>")
        print(f"Collections: {known_collections}")
        sys.exit(1)

    prepare_collections(sys.argv[1:])
