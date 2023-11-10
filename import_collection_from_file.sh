#!/bin/sh

filepath=$1; shift
filename=`basename $filepath`
collection=${filename%%.*}
type=${filename##*.}

arg_csv=( --ignoreBlanks --headerline )
arg_json=( --jsonArray )

set -x

mongoimport \
    --db "MiniMovieDB" \
    --collection "$collection" \
    --type $type \
    --file "$filepath" \
    --stopOnError \
    --numInsertionWorkers=8 \
    `if [ $type == "csv" ]; then echo ${arg_csv[@]}; else echo ${arg_json[@]}; fi` \
    $@

set +x
