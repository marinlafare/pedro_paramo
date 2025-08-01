# database.operations.models.py

from pydantic import BaseModel
from typing import Optional

class VersionCreateData(BaseModel):
    version: str
    author: str
    year:int
    editorial:str
    ISBN:int
    metadata:str
    raw_text:str
class VersionParagraph(BaseModel):
    version:str
    n_paragraph:int
    text:str
    n_words:int