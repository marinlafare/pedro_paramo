# pedro_paramo_api/database/ask_db.py

import asyncpg
from sqlalchemy import text, select
from sqlalchemy.ext.asyncio import AsyncSession
from urllib.parse import urlparse
from typing import Tuple, Set, List, Dict, Any, Union
import ast
import numpy as np
import re # Import the re module for regular expressions


# Assuming DBInterface and Version are defined elsewhere or imported correctly
# from .db_interface import DBInterface
# from .models import Version # Ensure this import is correct

        
async def open_request(session: AsyncSession,
                       sql_question: str,
                       params: Union[Tuple[Any, ...], Dict[str, Any], None] = None,
                       fetch_as_dict: bool = False) -> Union[List[Dict[str, Any]], List[Tuple[Any, ...]], None]:

    try:
        async with session.begin():
            result = await session.execute(text(sql_question), params)

            if result.returns_rows:
                if fetch_as_dict:
                    column_names = result.keys()
                    results = [dict(zip(column_names, row)) for row in result.fetchall()]
                    return results
                else:
                    return result.fetchall()
            else:
                return None
    except Exception as e:
        print(f"Error in open_request: {e}")
        raise

async def get_n_paragraph(session: AsyncSession, version: str, n_paragraph: int):
    n_paragraph = int(n_paragraph)
    data = await open_request(session,
                              """SELECT text FROM paragraph WHERE n_paragraph = :n_p
                                 AND version_name = :v_n;
                              """, params = {"n_p":n_paragraph,"v_n":version})
    if not data:
        return f"this paragraph: {n_paragraph} doesn't exist"
    return data[0][0]

async def get_n_paragraph_embedding(session: AsyncSession, version: str, n_paragraph: int):
    n_paragraph = int(n_paragraph)
    data = await open_request(session,
                              """SELECT embedding FROM paragraph WHERE n_paragraph = :n_p
                                 AND version_name = :v_n;
                              """, params = {"n_p":n_paragraph,"v_n":version})
    if not data:
        return f"this paragraph: {n_paragraph} doesn't exist"
    raw_embedding_value = data[0][0]

    try:
        if isinstance(raw_embedding_value, (list, np.ndarray)):
            embedding_list = raw_embedding_value
        else:
            numbers_str = re.findall(r"[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?", str(raw_embedding_value))
            embedding_list = [float(num) for num in numbers_str]
        return embedding_list
    except (ValueError, TypeError, SyntaxError) as e:
        return f"Error parsing embedding for paragraph {n_paragraph} in version {version}: {e}"


async def get_all_embeddings(session: AsyncSession, version: str) -> Union[np.ndarray, str]:
    """
    Retrieves all embeddings for a given version, returning them
    as a NumPy array (matrix), sorted by n_paragraph. Ensures embeddings are
    parsed as Python lists (vectors) before converting to NumPy.
    """
    query = """
        SELECT n_paragraph, embedding FROM paragraph
        WHERE version_name = :v_n
        ORDER BY n_paragraph;
    """
    data = await open_request(session, query, params={"v_n": version})

    if not data:
        return f"This version: {version} doesn't exist or has no embeddings."

    embeddings_list = []
    for item in data:
        n_paragraph = item[0]
        raw_embedding_value = item[1]

        try:
            if isinstance(raw_embedding_value, (list, np.ndarray)):
                embedding_list_item = raw_embedding_value
            else:
                numbers_str = re.findall(r"[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?", str(raw_embedding_value))
                embedding_list_item = [float(num) for num in numbers_str]
            embeddings_list.append(embedding_list_item)
        except (ValueError, TypeError, SyntaxError) as e:
            print(f"Warning: Could not parse embedding for paragraph {n_paragraph} in version {version}. Error: {e}. Skipping this embedding.")
            continue

    if not embeddings_list:
        return f"No valid embeddings found for version: {version} after parsing."

    return np.array(embeddings_list, dtype=np.float32)


async def get_all_umap_embeddings(session: AsyncSession, version: str) -> Union[np.ndarray, str]:
    """
    Retrieves all UMAP embeddings for a given version, returning them
    as a NumPy array (matrix), sorted by n_paragraph. Ensures UMAP embeddings are
    parsed as Python lists (vectors) before converting to NumPy.
    """
    query = """
        SELECT n_paragraph, umap FROM paragraph
        WHERE version_name = :v_n
        ORDER BY n_paragraph;
    """
    data = await open_request(session, query, params={"v_n": version})

    if not data:
        return f"This version: {version} doesn't exist or has no UMAP embeddings."

    umap_embeddings_list = []
    for item in data:
        n_paragraph = item[0]
        raw_umap_embedding_value = item[1]

        try:
            if isinstance(raw_umap_embedding_value, (list, np.ndarray)):
                umap_embedding_list_item = raw_umap_embedding_value
            else:
                numbers_str = re.findall(r"[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?", str(raw_umap_embedding_value))
                umap_embedding_list_item = [float(num) for num in numbers_str]
            umap_embeddings_list.append(umap_embedding_list_item)
        except (ValueError, TypeError, SyntaxError) as e:
            print(f"Warning: Could not parse UMAP embedding for paragraph {n_paragraph} in version {version}. Error: {e}. Skipping this embedding.")
            continue

    if not umap_embeddings_list:
        return f"No valid UMAP embeddings found for version: {version} after parsing."

    return np.array(umap_embeddings_list, dtype=np.float32)


async def get_n_paragraph_umap(session: AsyncSession, version: str, n_paragraph: int) -> Union[List[float], str]:
    n_paragraph = int(n_paragraph)

    query = """
        SELECT umap FROM paragraph
        WHERE n_paragraph = :n_p AND version_name = :v_n;
    """
    data = await open_request(session, query, params={"n_p": n_paragraph, "v_n": version})

    if not data:
        return f"This paragraph: {n_paragraph} in version: {version} doesn't exist."

    raw_umap_embedding_value = data[0][0]

    try:
        if isinstance(raw_umap_embedding_value, (list, np.ndarray)):
            umap_embedding_list = raw_umap_embedding_value
        else:
            numbers_str = re.findall(r"[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?", str(raw_umap_embedding_value))
            umap_embedding_list = [float(num) for num in numbers_str]
        return umap_embedding_list
    except (ValueError, SyntaxError, TypeError) as e:
        return f"Error parsing UMAP embedding for paragraph {n_paragraph} in version {version}: {e}"
