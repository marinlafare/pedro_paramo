# database/database/engine.py

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from .models import Base
from constants import CONN_STRING # Assuming CONN_STRING is defined here
import asyncio
import asyncpg
from urllib.parse import urlparse
from typing import Optional # Import Optional for the new argument type

# Global variables for the engine and sessionmaker
async_engine = None
AsyncDBSession = sessionmaker(expire_on_commit=False, class_=AsyncSession)

async def init_db(connection_string: Optional[str] = None):
    """
    Initializes the database engine, checks for database existence,
    creates it if necessary, and ensures tables are created.
    
    Args:
        connection_string (Optional[str]): The database connection string.
                                           If None, it defaults to CONN_STRING.
    """
    global async_engine

    # Use CONN_STRING if no connection_string is provided
    db_connection_string = connection_string if connection_string is not None else CONN_STRING
    
    if not db_connection_string:
        raise ValueError("Connection string is not provided and CONN_STRING is not set.")

    parsed_url = urlparse(db_connection_string)
    db_user = parsed_url.username
    db_password = parsed_url.password
    db_host = parsed_url.hostname
    db_port = parsed_url.port
    db_name = parsed_url.path.lstrip('/')

    temp_conn = None
    try:
        temp_conn = await asyncpg.connect(
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
            database='postgres' # Connect to a default database to perform creation
        )
        
        # Check if the target database exists
        db_exists_query = f"SELECT 1 FROM pg_database WHERE datname='{db_name}'"
        db_exists = await temp_conn.fetchval(db_exists_query)

        if not db_exists:
            print(f"Database '{db_name}' does not exist. Creating...")
            # Ensure the database name is correctly quoted for safety
            await temp_conn.execute(f'CREATE DATABASE "{db_name}"')
            print(f"Database '{db_name}' created.")
        else:
            print(f"Database '{db_name}' already exists.")

    except asyncpg.exceptions.DuplicateDatabaseError:
        print(f"Database '{db_name}' already exists (concurrent creation).")
    except Exception as e:
        print(f"Error during database existence check/creation: {e}")
        raise
    finally:
        if temp_conn:
            await temp_conn.close() # Ensure the temporary connection is closed

    async_engine = create_async_engine(db_connection_string, echo=False)
    
    async with async_engine.begin() as conn:
        print("Ensuring database tables exist...")
        await conn.run_sync(Base.metadata.create_all)
        print("Database tables checked/created.")
        
    AsyncDBSession.configure(bind=async_engine)
    print("Database initialization complete.")

# --- NEW: Asynchronous dependency to get a database session ---
async def get_db_session() -> AsyncSession:
    """
    Dependency function to get an asynchronous database session.
    This can be used in FastAPI routes or directly in async scripts.
    """
    if async_engine is None:
        # This case should ideally be handled by calling init_db() at startup
        # For robustness, you might add a warning or raise an error here
        # if init_db() is expected to have run already.
        print("Warning: async_engine not initialized. Calling init_db() now.")
        await init_db() # Attempt to initialize if not already done

    async with AsyncDBSession() as session:
        yield session

