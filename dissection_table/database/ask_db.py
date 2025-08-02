# database/database/ask_db.py

import asyncpg
from .db_interface import DBInterface
from .models import Version 
from sqlalchemy import text, select 
from urllib.parse import urlparse
from typing import Tuple, Set, List, Dict, Any, Union
import ast
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
    
    This version correctly handles transactions to ensure DDL and DML
    statements are committed.
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
    print(data)
    return data[0][0]

from collections import OrderedDict
from collections import OrderedDict
# Assuming open_request is defined and correctly handles async DB queries
# and returns a list of tuples/rows, e.g., [(1, [...]), (2, [...]), ...]

async def get_all_embeddings(version):
    """
    Retrieves all embeddings for a given version, returning them
    as an OrderedDict sorted by n_paragraph. Ensures embeddings are
    parsed as Python lists (vectors).

    Args:
        version (str): The name of the version to retrieve embeddings for.

    Returns:
        OrderedDict: A dictionary where keys are n_paragraph (int) and
                     values are embeddings (list of floats).
        str: An error message if the version doesn't exist or no data is found.
    """
    
    query = """
        SELECT n_paragraph, embedding FROM paragraph
        WHERE version_name = :v_n;
    """
    
    # open_request is expected to return a list of Row/tuple objects,
    # e.g., [(n_paragraph_int, embedding_value_from_db), ...]
    data = await open_request(query, params={"v_n": version})
    
    if not data:
        return f"This version: {version} doesn't exist or has no paragraphs."
    
    # Sort the data by 'n_paragraph' (which is at index 0)
    sorted_data = sorted(data, key=lambda x: x[0])
    
    # Create an OrderedDict from the sorted data.
    ordered_embeddings = OrderedDict()
    for item in sorted_data:
        n_paragraph = item[0]
        raw_embedding_value = item[1] # This could be a string or already a list/array
        
        try:
            # Attempt to parse the embedding. If it's already a list, literal_eval will return it as is.
            # If it's a string representation of a list, it will parse it.
            embedding_list = ast.literal_eval(str(raw_embedding_value))
            ordered_embeddings[n_paragraph] = embedding_list
        except (ValueError, SyntaxError) as e:
            print(f"Warning: Could not parse embedding for paragraph {n_paragraph} in version {version}. Error: {e}")
            # Decide how to handle unparseable entries: skip, set to None, etc.
            # For now, we'll skip it.
            continue 
            
    return ordered_embeddings

async def get_all_umap_embeddings(version):
    """
    Retrieves all UMAP embeddings for a given version, returning them
    as an OrderedDict sorted by n_paragraph. Ensures UMAP embeddings are
    parsed as Python lists (vectors).

    Args:
        version (str): The name of the version to retrieve UMAP embeddings for.

    Returns:
        OrderedDict: A dictionary where keys are n_paragraph (int) and
                     values are UMAP embeddings (list of floats).
        str: An error message if the version doesn't exist or no data is found.
    """
    
    query = """
        SELECT n_paragraph, umap FROM paragraph
        WHERE version_name = :v_n;
    """
    
    # open_request is expected to return a list of Row/tuple objects,
    # e.g., [(n_paragraph_int, umap_embedding_value_from_db), ...]
    data = await open_request(query, params={"v_n": version})
    
    if not data:
        return f"This version: {version} doesn't exist or has no UMAP embeddings."
    
    # Sort the data by 'n_paragraph' (which is at index 0)
    sorted_data = sorted(data, key=lambda x: x[0])
    
    # Create an OrderedDict from the sorted data.
    ordered_umap_embeddings = OrderedDict()
    for item in sorted_data:
        n_paragraph = item[0]
        raw_umap_embedding_value = item[1] # This could be a string or already a list/array

        try:
            # Attempt to parse the UMAP embedding.
            umap_embedding_list = ast.literal_eval(str(raw_umap_embedding_value))
            ordered_umap_embeddings[n_paragraph] = umap_embedding_list
        except (ValueError, SyntaxError) as e:
            print(f"Warning: Could not parse UMAP embedding for paragraph {n_paragraph} in version {version}. Error: {e}")
            # Decide how to handle unparseable entries: skip, set to None, etc.
            # For now, we'll skip it.
            continue 
        
    return ordered_umap_embeddings