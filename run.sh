#!/bin/sh


setup_venv(){
    python3 -m venv venv
    source venv/bin/activate
    python -m pip install -r requirements.txt
}


activate_venv(){
    if [ ! -d "venv" ]; then
        setup_venv
    else
        source ./venv/bin/activate
    fi

    if ! git --no-pager diff --quiet requirements.txt; then
        python -m pip install -r requirements.txt
    fi
}


run_server(){
    python -m uvicorn src.main:app --reload
}


SRC_DIR=`dirname "$0"`

pushd $SRC_DIR

activate_venv
#run_server

popd
