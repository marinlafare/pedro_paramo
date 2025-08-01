# database.routers.frequencies.py

from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from dissection_table.operations.frequencies import get_n_words, get_paragraph_words_freq
#from dissection_table.operations.players import get_current_players_with_games_in_db
#from typing import Dict, Any

router = APIRouter()

@router.get("/{version}/n_words")
async def api_get_n_words(version):
    return await get_n_words(version)

@router.get("/{version}/paragraph_words_freq")
async def api_get_paragraph_words_freq(version):
    return await get_paragraph_words_freq(version)
