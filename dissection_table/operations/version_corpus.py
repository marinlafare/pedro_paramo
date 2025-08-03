#dissection_table.operations.version_corpus.py

from dissection_table.operations.sources import *
from dissection_table.operations.frequencies import *
from dissection_table.database.ask_db import *

class Corpus:
    @classmethod
    async def create(cls, version):
        version_data = await get_complete_version(version)
        return cls(version=version, version_data=version_data)
    def __init__(self, version, version_data):
        self.version = version
        self.author = version_data['author']
        self.year = version_data['year']
        self.editorial = version_data['editorial']
        self.ISBN = version_data['ISBN']
        self.metadata = version_data['version_data']
        self.text = version_data['raw_text']
        self.n_words = version_data['n_words']
        self.original = version_data['n_paragraphs']
        self.word_set = version_data['word_set']
        
    async def word_freq(self):
        return await get_word_freq_dict(self.version)
    async def int_to_word(self):
        word_freq = await get_word_freq_dict(self.version)
        return dict(zip(range(len(word_freq)), word_freq.keys()))
    async def word_to_int(self):
        word_freq = await get_word_freq_dict(self.version)
        return dict(zip(word_freq.keys(),range(len(word_freq))))
    async def all_paragraphs(self):
        return await get_paragraphs(self.version)
    async def all_embeddings(self):
        return await get_all_embeddings(self.version)
    async def all_umap(self):
        return await get_all_umap_embeddings(self.version)
    async def n_paragraph(self,n_paragraph):
        return await get_n_paragraph(self.version, n_paragraph)
    async def n_paragraph_embedding(self, n_paragraph):
        return await get_n_paragraph_embedding(self.version, n_paragraph)
    async def n_paragraph_umap(self, n_paragraph):
        return await get_n_paragraph_umap(self.version, n_paragraph)