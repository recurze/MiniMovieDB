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
    filter = {"primaryTitle": title}
    if request.query_params:
        if request.query_params.get("genres", ""):
            filter["genres"] = {"$all": request.query_params["genres"].split(',')}
    if (shows := request.app.database["shows"].find(filter)) is not None:
        return shows
    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Show with title {title} not found")
