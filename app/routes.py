from fastapi import (
    APIRouter,
    Body,
    Request,
    HTTPException,
    status
)
from fastapi.encoders import jsonable_encoder
from pymongo.errors import DuplicateKeyError
from typing import List
import re


show_router = APIRouter()


@show_router.post("/", response_description="Insert new show", status_code=status.HTTP_201_CREATED, response_model=List)
def create_show(request: Request, show=Body(...)):
    show = jsonable_encoder(show)
    try:
        new_show = request.app.database["shows"].insert_one(show)
    except DuplicateKeyError:
        raise HTTPException(status_code=400, detail=f"Item with _id={show.get('_id')} already exists.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
    return request.app.database["shows"].find_one(
        {"_id": new_show.inserted_id}
    )


@show_router.get("/", response_description="List all shows", response_model=List)
def list_shows(request: Request):
    return list(request.app.database["shows"].find(limit=10))


@show_router.get("/{id}", response_description="Get a single show by _id", response_model=dict)
def find_show_by_id(id: str, request: Request):
    if (show := request.app.database["shows"].find_one({"_id": id})) is not None:
        return show
    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Show with ID {id} not found")


@show_router.get("/search/{title}", response_description="Get shows by title", response_model=List)
def find_show_by_title(title: str, request: Request):
    if title == "\"\"":
        filter = {}
    elif title.startswith('"'):
        filter = {"primaryTitle": title[1: -1]}
    else:
        filter = {"primaryTitle": re.compile(title, re.IGNORECASE)}

    sort = []
    if request.query_params:
        if request.query_params.get("genres"):
            filter["genres"] = {"$all": request.query_params["genres"].split(',')}
        if request.query_params.get("titleType"):
            filter["titleType"] = request.query_params["titleType"]
        if request.query_params.get("start"):
            filter["startYear"] = int(request.query_params["start"])

        sortby = request.query_params.get("sortby", "")
        if sortby == "ratings":
            sort = [("averageRatings", -1)]
        elif sortby == "popularity":
            sort = [("numVotes", -1)]

    if (shows := request.app.database["shows"].find(filter=filter, sort=sort)) is not None:
        return shows.limit(30)
    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Show with title {title} not found")


@show_router.get("/showsby/{name}", response_description="Get shows by person", response_model=List)
def find_show_by_person(name: str, request: Request):
    pipeline = [
        {
            "$match": {
                "name": name
            }
        },
        {
            "$unwind": "$knownFor"
        },
        {
            "$lookup": {
                "from": "shows",
                "localField": "knownFor",
                "foreignField": "_id",
                "as": "show_info"
            }
        },
        {
            "$project": {
                "_id": "$show_info._id",
                "primaryTitle": "$show_info.primaryTitle",
                "genres": "$show_info.genres",
                "startYear": "$show_info.startYear",
                "plot": "$show_info.plot",
                "titleType": "$show_info.titleType",
                "averageRating": "$show_info.averageRating",
                "numVotes": "$show_info.numVotes",
            }
        }
    ]

    if (shows := request.app.database["people"].aggregate(pipeline)) is not None:
        return shows
    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Show with title {title} not found")
