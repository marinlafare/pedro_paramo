# database/database/ask_db.py

import asyncpg
from .db_interface import DBInterface
from .models import Version 
from sqlalchemy import text, select 
from urllib.parse import urlparse
from typing import Tuple, Set, List, Dict, Any, Union
import ast
import numpy as np
async def get_async_db_session():
    """
    Provides an asynchronous database session using DBInterface.

    This function is the asynchronous SQLAlchemy equivalent to
    getting a database connection. It returns an async context manager
    for a database session.
    
    """
    session_interface = DBInterface(Version)
    return session_interface.AsyncSessionLocal()


async def open_request(sql_question: str,
                       params: Union[Tuple[Any, ...], Dict[str, Any], None] = None,
                       fetch_as_dict: bool = False) -> Union[List[Dict[str, Any]], List[Tuple[Any, ...]], None]:
    """
    Executes a SQL query asynchronously using SQLAlchemy's AsyncSession.
    """
    async with await get_async_db_session() as session:
        try:
            # Use async with session.begin() to start and manage a transaction
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
            # The transaction will be automatically rolled back on an exception
            print(f"Error in open_request: {e}")
            raise
async def get_n_paragraph(version, n_paragraph):
    n_paragraph = int(n_paragraph)
    data = await open_request("""SELECT text FROM paragraph WHERE n_paragraph = :n_p
                    AND version_name = :v_n;
        """, params = {"n_p":n_paragraph,"v_n":version})
    if len(data)<1:
        return f"this paragraph: {n_paragraph} doesn't exist"
    print(data)
    return data[0][0]

async def get_n_paragraph_embedding(version, n_paragraph):
    n_paragraph = int(n_paragraph)
    data = await open_request("""SELECT embedding FROM paragraph WHERE n_paragraph = :n_p
                    AND version_name = :v_n;
        """, params = {"n_p":n_paragraph,"v_n":version})
    if len(data)<1:
        return f"this paragraph: {n_paragraph} doesn't exist"
    raw_embedding_value = data[0][0]
    
    try:
        # Attempt to parse the UMAP embedding.
        # It handles both string representations and already-parsed list/array types.
        embedding_list = ast.literal_eval(str(raw_embedding_value))
        return embedding_list
    except (ValueError, SyntaxError) as e:
        return f"Error parsing UMAP embedding for paragraph {n_paragraph} in version {version}: {e}"

from collections import OrderedDict
async def get_all_embeddings(version):
    """
    Retrieves all embeddings for a given version, returning them
    as a NumPy array (matrix), sorted by n_paragraph. Ensures embeddings are
    parsed as Python lists (vectors) before converting to NumPy.

    Args:
        version (str): The name of the version to retrieve embeddings for.

    Returns:
        np.ndarray: A 2D NumPy array where each row is an embedding vector,
                    sorted by their original n_paragraph.
        str: An error message if the version doesn't exist or no data is found.
    """
    
    query = """
        SELECT n_paragraph, embedding FROM paragraph
        WHERE version_name = :v_n;
    """
    
    data = await open_request(query, params={"v_n": version})
    
    if not data:
        return f"This version: {version} doesn't exist or has no paragraphs."
    
    # Sort the data by 'n_paragraph' (which is at index 0)
    sorted_data = sorted(data, key=lambda x: x[0])
    
    embeddings_list = []
    for item in sorted_data:
        n_paragraph = item[0]
        raw_embedding_value = item[1]
        
        try:
            # Attempt to parse the embedding string into a Python list
            embedding_list_item = ast.literal_eval(str(raw_embedding_value))
            embeddings_list.append(embedding_list_item)
        except (ValueError, SyntaxError) as e:
            print(f"Warning: Could not parse embedding for paragraph {n_paragraph} in version {version}. Error: {e}. Skipping this embedding.")
            continue
            
    if not embeddings_list:
        return f"No valid embeddings found for version: {version} after parsing."

    # Convert the list of embedding lists into a NumPy array (matrix)
    return np.array(embeddings_list, dtype=np.float32) # Specify dtype for consistency and memory efficiency

async def get_all_umap_embeddings(version):
    """
    Retrieves all UMAP embeddings for a given version, returning them
    as a NumPy array (matrix), sorted by n_paragraph. Ensures UMAP embeddings are
    parsed as Python lists (vectors) before converting to NumPy.

    Args:
        version (str): The name of the version to retrieve UMAP embeddings for.

    Returns:
        np.ndarray: A 2D NumPy array where each row is a UMAP embedding vector,
                    sorted by their original n_paragraph.
        str: An error message if the version doesn't exist or no data is found.
    """
    
    query = """
        SELECT n_paragraph, umap FROM paragraph
        WHERE version_name = :v_n;
    """
    
    data = await open_request(query, params={"v_n": version})
    
    if not data:
        return f"This version: {version} doesn't exist or has no UMAP embeddings."
    
    # Sort the data by 'n_paragraph' (which is at index 0)
    sorted_data = sorted(data, key=lambda x: x[0])
    
    umap_embeddings_list = []
    for item in sorted_data:
        n_paragraph = item[0]
        raw_umap_embedding_value = item[1]

        try:
            # Attempt to parse the UMAP embedding string into a Python list
            umap_embedding_list_item = ast.literal_eval(str(raw_umap_embedding_value))
            umap_embeddings_list.append(umap_embedding_list_item)
        except (ValueError, SyntaxError) as e:
            print(f"Warning: Could not parse UMAP embedding for paragraph {n_paragraph} in version {version}. Error: {e}. Skipping this embedding.")
            continue
            
    if not umap_embeddings_list:
        return f"No valid UMAP embeddings found for version: {version} after parsing."

    # Convert the list of UMAP embedding lists into a NumPy array (matrix)
    return np.array(umap_embeddings_list, dtype=np.float32) # Specify dtype for consistency and memory efficiency

async def get_n_paragraph_umap(version, n_paragraph):
    n_paragraph = int(n_paragraph) # Ensure n_paragraph is an integer
    
    query = """
        SELECT umap FROM paragraph
        WHERE n_paragraph = :n_p AND version_name = :v_n;
    """

    data = await open_request(query, params={"n_p": n_paragraph, "v_n": version})
    
    if not data:
        return f"This paragraph: {n_paragraph} in version: {version} doesn't exist."
    
    # The result is expected to be a list containing one tuple,
    # and that tuple contains the raw UMAP embedding value (at index 0).
    raw_umap_embedding_value = data[0][0]
    
    try:
        # Attempt to parse the UMAP embedding.
        # It handles both string representations and already-parsed list/array types.
        umap_embedding_list = ast.literal_eval(str(raw_umap_embedding_value))
        return umap_embedding_list
    except (ValueError, SyntaxError) as e:
        return f"Error parsing UMAP embedding for paragraph {n_paragraph} in version {version}: {e}"
