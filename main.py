# main.py
import os
from dotenv import load_dotenv
from fastapi import FastAPI
from contextlib import asynccontextmanager
from constants import CONN_STRING
from dissection_table.database.engine import init_db
from dissection_table.database.db_interface import DBInterface
from dissection_table.database.sources_formatting import feed_database
from dissection_table.routers import paragraph, sources, frequencies


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db(CONN_STRING)
    DBInterface.initialize_engine_and_session(CONN_STRING)
    try:
        await feed_database()
        print(" DATABASE POPULATED JUST NOW ")
    except:
        print('Either Pedro_Paramo_Database is ready:\n   or there was a problem and is not. lol')
    print('... DISSECTION TABLE ON ... (allegedly)')
    yield
    print('... Server DISSECTION TABLE DOWN YO!...')

app = FastAPI(lifespan=lifespan)

@app.get("/")
def read_root():
    return '... DISSECTION TABLE RUNNING YO ...'

app.include_router(paragraph.router)
app.include_router(sources.router)
app.include_router(frequencies.router)

