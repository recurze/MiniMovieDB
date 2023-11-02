from fastapi import FastAPI
from pymongo import MongoClient


app = FastAPI()

@app.on_event("startup")
def startup():
    app.client = MongoClient("localhost", 27017)
    app.client.admin.command("ping")

    app.database = app.client["MiniMovieDb"]


@app.on_event("shutdown")
def shutdown():
    app.database = None
    app.client.close()
