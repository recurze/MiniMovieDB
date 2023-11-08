from imdb import Cinemagoer
import csv
import multiprocessing
import pandas as pd
import pathlib


cinemagoer = Cinemagoer()


def get_plot(tid):
    try:
        plot = cinemagoer.get_movie(tid[2:], info=["plot"]).get("plot", " ")[0].strip().strip('"')
    except Exception:
        plot = ""

    return tid, plot


if __name__ == "__main__":
    plots = {}
    num_votes_threshold = 2000  # rougly 50k titles

    filepath = pathlib.PurePath("data", "imdb", "title.ratings.tsv")
    df = pd.read_csv(filepath, delimiter='\t')
    tids = df[df.numVotes > num_votes_threshold].tconst.tolist()

    with open('plots.csv', 'w') as f:
        writer = csv.writer(f)
        writer.writerow(["tconst", "plot"])

        for i in range(0, len(tids), 400):
            with multiprocessing.Pool() as pool:
                plots = pool.map(get_plot, tids[i: i + 400])

            writer.writerows(plots)
            print(i + 400)
