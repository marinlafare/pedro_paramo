# database.routers.paragraphs.py

from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from dissection_table.operations.paragraph import get_paragraph
#from dissection_table.operations.players import get_current_players_with_games_in_db
#from typing import Dict, Any

router = APIRouter()

@router.get("/paragraph/{version}/{n_paragraph}")
async def api_get_paragraph(version, n_paragraph):
    return await get_paragraph(version,n_paragraph)

