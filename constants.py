# constants.py

import os
from dotenv import load_dotenv
from pathlib import Path

# --- env stuff ---
current_script_dir = Path(__file__).resolve().parent
env_file_name = '.env'
env_file_absolute_path = current_script_dir / env_file_name
load_dotenv(env_file_absolute_path, override=True)

USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")
HOST = os.getenv('HOST') # This will come from Docker Compose (host.docker.internal)
PORT = os.getenv("PORT")
DATABASE_NAME = os.getenv("DATABASE_NAME")


CONN_STRING_TEMPLATE = "postgresql+asyncpg://{user}:{password}@{host}:{port}/{database_name}"
CONN_STRING = CONN_STRING_TEMPLATE.replace('{user}', USER)
CONN_STRING = CONN_STRING.replace('{host}', HOST)
CONN_STRING = CONN_STRING.replace('{password}', PASSWORD)
CONN_STRING = CONN_STRING.replace('{port}', PORT) # Convert back to string for replacement
CONN_STRING = CONN_STRING.replace('{database_name}', DATABASE_NAME)
