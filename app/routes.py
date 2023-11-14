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
def find_shows_by_person(name: str, request: Request):
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
    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Shows by {name} not found")


@show_router.get("/similarto/{title}", response_description="Get recommendation based on shows", response_model=List)
def find_shows_similar_to(title: str, request: Request):
    pipeline = [
        {
            "$match": {
                "primaryTitle": title
            }
        },
        {
            "$lookup": {
                "from": "show_tags",
                "localField": "_id",
                "foreignField": "id",
                "as": "tag_info"
            }
        },
        {
            "$lookup": {
                "from": "show_tags",
                "localField": "tag_info.tag",
                "foreignField": "tag",
                "as": "recommendation"
            }
        },
        {
            "$unwind": "$recommendation"
        },
        {
            "$match": {
                "expr": {
                    "$ne": ["recommendation.id", "_id"]
                }
            }
        },
        {
            "$group": {
                "_id": "$recommendation.id",
                "numTagMatches": {
                    "$sum": 1
                }
            }
        },
        {
            "$sort": {
                "numTagMatches": -1
            }
        },
        {
            "$lookup": {
                "from": "shows",
                "localField": "_id",
                "foreignField": "_id",
                "as": "show_info"
            }
        },
        {
            "$unwind": "$show_info"
        },
        {
        "$match": {
            "expr": {
                "$ne": ["$show_info.primaryTitle", "title"]
                }
            }
        },
        {
            "$replaceRoot": {
                "newRoot": "$show_info"
            }
        }
    ]

    if (shows := request.app.database["shows"].aggregate(pipeline)) is not None:
        documents = [document for document in shows]
        return documents[1: 21] if documents else []
    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Show with similar tags not found")


@show_router.get("/user/{userId}", response_description="Get user ratings", response_model=List)
def get_user_ratings(userId: str, request: Request):
    pipeline = [
        {
            "$match": {
                "userId": int(userId)
            }
        },
        {
            "$lookup": {
                "from": "shows",
                "localField": "imdbId",
                "foreignField": "_id",
                "as": "show_info"
            }
        },
        {
            "$unwind": "$show_info"
        },
        {
            "$project": {
                "_id": "$show_info._id",
                "primaryTitle": "$show_info.primaryTitle",
                "genres": "$show_info.genres",
                "startYear": "$show_info.startYear",
                "plot": "$show_info.plot",
                "titleType": "$show_info.titleType",
                "averageRating": "$rating",
                "numVotes": "1",
                "timestamp": {
                    "$toDate": {
                        "$multiply": ["$timestamp", 1000]
                    }
                }
            }
        }
    ]
    sortby = request.query_params.get("sortby", "")
    if sortby == "alphabetical":
        pipeline.append({
            "$sort": {
                "primaryTitle": 1
            }
        })
    elif sortby == "ratings":
        pipeline.append({
            "$sort": {
                "averageRating": -1
            }
        })
    elif sortby == "timestamp":
        pipeline.append({
            "$sort": {
                "timestamp": -1
            }
        })

    if (shows := request.app.database["user_ratings"].aggregate(pipeline)) is not None:
        return shows
    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"User with userId {userId} not found")
