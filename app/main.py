from fastapi import FastAPI
from pymongo import MongoClient

from routes import show_router


app = FastAPI()


@app.on_event("startup")
def startup_db_client():
    app.client = MongoClient("localhost", 27017)
    app.client.admin.command("ping")

    app.database = app.client["MiniMovieDB"]
    print("Connected to the MiniMovieDB database!")


@app.on_event("shutdown")
def shutdown_db_client():
    app.client.close()


app.include_router(show_router, tags=["show"], prefix="/show")
