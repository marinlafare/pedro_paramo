# database.router.sources.py

from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from dissection_table.operations.sources import get_versions_names, get_raw_text, get_metadata
#from dissection_table.operations.players import get_current_players_with_games_in_db
#from typing import Dict, Any

router = APIRouter()

@router.get("/sources/")
async def api_get_version_names():
    return await get_versions_names()
@router.get("/sources/raw_text/{version}")
async def api_get_raw_text(version):
    return await get_raw_text(version)
@router.get("/sources/metadata/{version}")
async def api_get_metadata(version):
    return await get_metadata(version) 
