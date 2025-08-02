# database.operations.paragraphs.py
from dissection_table.database.ask_db import *
async def get_paragraph(version, n_paragraph):
    paragraph =await  get_n_paragraph(version, n_paragraph)
    return paragraph
async def get_paragraph_embedding(version, n_paragraph):
    paragraph =await  get_n_paragraph_embedding(version, n_paragraph)
    return paragraph
