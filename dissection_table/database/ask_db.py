# database/database/ask_db.py

import asyncpg
from .db_interface import DBInterface
from .models import Version 
from sqlalchemy import text, select 
from urllib.parse import urlparse
from typing import Tuple, Set, List, Dict, Any, Union

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




    