# dissection_table.operations.version_corpus.py

from sqlalchemy.ext.asyncio import AsyncSession # Still needed for type hinting in __init__ and create
from typing import Dict, Any, List, Set, Optional, Union
import numpy as np
import ast
import re # Make sure re is imported for string parsing

# Import your database operations
from dissection_table.operations.sources import (
    get_complete_version,
    get_raw_text,
    get_paragraphs,
    get_metadata,
    get_versions_names
)
from dissection_table.operations.frequencies import (
    get_word_freq_dict,
    get_n_words,
    get_paragraph_words_freq
)
from dissection_table.database.ask_db import (
    get_all_embeddings,
    get_all_umap_embeddings,
    get_n_paragraph,
    get_n_paragraph_embedding,
    get_n_paragraph_umap
)


class Corpus:
    def __init__(self, session: AsyncSession, version: str, version_data: Dict[str, Any]): # ADDED session to __init__
        self.session = session # Store the session as an instance attribute
        self.version = version
        self.author = version_data.get('author')
        self.year = version_data.get('year')
        self.editorial = version_data.get('editorial')
        self.ISBN = version_data.get('ISBN')
        self.metadata = version_data.get('version_data')
        self.text = version_data.get('raw_text')
        self.n_words = version_data.get('n_words')
        self.n_paragraphs = version_data.get('n_paragraphs') # Corrected attribute name

        raw_word_set = version_data.get('word_set')
        if isinstance(raw_word_set, str):
            self.word_set = {word for word in raw_word_set.split('#') if len(word)>0}
        else:
            self.word_set = raw_word_set if isinstance(raw_word_set, set) else set()

        self.raw_words = version_data.get('raw_words')
        self.text_embedding = version_data.get('text_embedding')
        self.text_umap = version_data.get('text_umap')


    @classmethod
    async def create(cls, session: AsyncSession, version: str):
        """
        Factory method to create a Corpus instance, fetching data from the database.
        It passes the session to the Corpus.__init__ method.
        """
        version_data = await get_complete_version(session, version)
        if not version_data:
            raise ValueError(f"Version '{version}' not found in the database.")
        return cls(session=session, version=version, version_data=version_data) # Pass session to __init__

    async def word_freq(self) -> Dict[str, int]: # REMOVED session from arguments
        """Retrieves word frequencies for the corpus version."""
        return await get_word_freq_dict(self.session, self.version) # Use self.session

    async def int_to_word(self) -> Dict[int, str]: # REMOVED session from arguments
        """Maps integer IDs to words based on word frequencies."""
        word_freq = await self.word_freq() # Call internal method without session arg
        return dict(zip(range(len(word_freq)), word_freq.keys()))

    async def word_to_int(self) -> Dict[str, int]: # REMOVED session from arguments
        """Maps words to integer IDs based on word frequencies."""
        word_freq = await self.word_freq() # Call internal method without session arg
        return dict(zip(word_freq.keys(), range(len(word_freq))))

    async def all_paragraphs(self) -> Dict[int, str]: # REMOVED session from arguments
        """Retrieves all paragraphs for the corpus version."""
        return await get_paragraphs(self.session, self.version) # Use self.session

    async def all_embeddings(self) -> np.ndarray: # REMOVED session from arguments
        """Retrieves all embeddings for the corpus version."""
        return await get_all_embeddings(self.session, self.version) # Use self.session

    async def all_umap(self) -> np.ndarray: # REMOVED session from arguments
        """Retrieves all UMAP embeddings for the corpus version."""
        return await get_all_umap_embeddings(self.session, self.version) # Use self.session

    async def n_paragraph(self, n_paragraph: int) -> Union[str, Any]: # REMOVED session from arguments
        """Retrieves text for a specific paragraph number."""
        return await get_n_paragraph(self.session, self.version, n_paragraph) # Use self.session

    async def n_paragraph_embedding(self, n_paragraph: int) -> Union[List[float], str]: # REMOVED session from arguments
        """Retrieves embedding for a specific paragraph number."""
        return await get_n_paragraph_embedding(self.session, self.version, n_paragraph) # Use self.session

    async def n_paragraph_umap(self, n_paragraph: int) -> Union[List[float], str]: # REMOVED session from arguments
        """Retrieves UMAP embedding for a specific paragraph number."""
        return await get_n_paragraph_umap(self.session, self.version, n_paragraph) # Use self.session
