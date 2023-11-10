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


def join_series(s1, s2):
    return pd.merge(
        s1,
        s2,
        how="left",
        left_index=True,
        right_index=True,
        validate="one_to_one",
    )


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


def shows_flat_attributes():
    def load_plots():
        filepath = pathlib.PurePath("data", "misc", "plots.csv")
        df = pd.read_csv(filepath, delimiter=',', quotechar='"').set_index("tconst")
        return df.drop(["taglines"], axis=1)

    def load_ratings():
        filepath = pathlib.PurePath("data", "imdb", "title.ratings.tsv")
        return read_csv(filepath).set_index("tconst")

    def load_basics():
        filepath = pathlib.PurePath("data", "imdb", "title.basics.tsv")
        df = read_csv(filepath).set_index("tconst")
        df.isAdult = df.isAdult.map({0: "no", 1: "yes"})
        return df.drop(["originalTitle", "genres"], axis=1)

    def load_episode_info():
        filepath = pathlib.PurePath("data", "imdb", "title.episode.tsv")
        return read_csv(filepath, ["tconst", "parentId", "seasonNumber", "episodeNumber"]).set_index("tconst")

    outfile = pathlib.PurePath("collections", "shows.csv")
    load_basics() \
        .join(load_ratings(), validate="one_to_one") \
        .join(load_plots(), validate="one_to_one") \
        .join(load_episode_info(), validate="one_to_one") \
        .rename_axis("_id") \
        .to_csv(outfile)


def shows_list_attributes():
    def load_genres():
        filepath = pathlib.PurePath("data", "imdb", "title.basics.tsv")
        df = read_csv(filepath).set_index("tconst")
        return df["genres"].fillna("").map(lambda x: x.split(','))

    def load_akas():
        filepath = pathlib.PurePath("data", "imdb", "title.akas")
        df = read_csv(filepath)[["titleId", "ordering", "title"]].sort_values("ordering")
        df = df.groupby("titleId").title.agg(lambda x: x.tolist())
        return df.rename("aka")

    def load_tags():
        filepath = pathlib.PurePath("data", "ml-25m", "links.csv")
        df_links = pd.read_csv(filepath, dtype={"movieId": int, "imdbId": str}).set_index("movieId")

        filepath = pathlib.PurePath("data", "ml-25m", "genome-tags.csv")
        df_tagnames = read_csv(filepath, delimiter=',').set_index("tagId")

        filepath = pathlib.PurePath("data", "ml-25m", "genome-scores.csv")
        df_tags = read_csv(filepath, delimiter=',')

        df_tags = df_tags.join(df_tagnames, on="tagId").join(df_links, on="movieId")

        df_tags = df_tags[["imdbId", "tag", "relevance"]].sort_values(["imdbId", "relevance"], ascending=False)
        df_tags = df_tags.groupby("imdbId")[["tag", "relevance"]].agg(lambda x: x.tolist()[:20])

        df_tags["tags"] = list(
            [{"tag": tag, "relevance": relevance} for tag, relevance in zip(x, y)]
            for x, y in zip(df_tags.tag, df_tags.relevance)
        )

        df_tags.index = df_tags.index.map(lambda s: "tt" + s)
        return df_tags["tags"].rename_axis("tconst")

    def load_actors():
        def process_characters(s):
            if not isinstance(s, str):
                return []
            return [char.strip() for char in s[1:-1].replace('"', '').split(',')]

        filepath = pathlib.PurePath("data", "imdb", "title.principals.tsv")
        df = read_csv(filepath)

        df = df[(df.category == "actor") | (df.category == "actress")].sort_values(["tconst", "ordering"])

        df["actors"] = list(
            {"id": x, "characters": process_characters(y)}
            for x, y in zip(df.nconst, df.characters)
        )
        return df.groupby("tconst")["actors"].agg(lambda x: x.dropna().tolist())

    def load_crew():
        filepath = pathlib.PurePath("data", "imdb", "title.principals.tsv")
        df = read_csv(filepath)

        df = df[(df.category != "actor") & (df.category != "actress")].sort_values(["tconst", "ordering"])

        df["crew"] = list(
            {"id": x, "category": y}
            for x, y in zip(df.nconst, df.category)
        )
        return df.groupby("tconst")["crew"].agg(lambda x: x.dropna().tolist())

    merged = join_series(
        join_series(
            load_genres(),
            load_akas()
        ),
        load_tags()
    ).rename_axis("_id").reset_index(level=0)

    for col in ["genres", "aka", "tags"]:
        merged[col] = merged[col].map(lambda x: x if isinstance(x, list) else [])

    outfile = pathlib.PurePath("collections", "shows.json")
    merged.to_json(outfile, orient="records", force_ascii=False)
    del merged

    actors = load_actors().rename_axis("_id").reset_index(level=0)

    outfile = pathlib.PurePath("collections", "shows.actors.json")
    actors.to_json(outfile, orient="records", force_ascii=False)
    del actors

    crew = load_crew().rename_axis("_id").reset_index(level=0)

    outfile = pathlib.PurePath("collections", "shows.crew.json")
    crew.to_json(outfile, orient="records", force_ascii=False)
    del crew


def shows():
    shows_flat_attributes()
    shows_list_attributes()


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
