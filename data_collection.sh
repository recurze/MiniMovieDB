#!/bin/sh

dl_imdb(){
    wget -r https://datasets.imdbws.com/
    mv https://datasets.imdbws.com/ imdb
}

extract_imdb(){
    pushd imdb
    for i in *.gz; do
        7z x $i && mv data.tsv ${i%.*};
    done
    popd
}

dl_movielens(){
    wget https://files.grouplens.org/datasets/movielens/ml-25m.zip
}

extract_movielens(){
    unzip ml-25m.zip
}

mkdir -p data
pushd data

dl_imdb
extract_imdb

dl_movielens
extract_movielens

popd
