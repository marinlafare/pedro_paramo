# database.operations.frequencies.py
from dissection_table.database.ask_db import *
import re

async def get_n_words(version):
    versions = await open_request("""
                                select version.raw_text from version
                                where version.version_name = :version
                                
                                """,
                                  params = {"version":version},
                                  fetch_as_dict=True)
    text = versions[0]['raw_text']
    n_words = len(text.split(' '))
    
    return n_words
async def get_paragraph_words_freq(version):
    versions = await open_request("""
                                select n_paragraph, paragraph.n_words from paragraph
                                where paragraph.version_name = :version
                                
                                """,
                                  params = {"version":version},
                                  fetch_as_dict=True)
    versions = {x['n_paragraph']:x['n_words'] for x in versions}
    
    
    return versions
async def get_word_freq_dict(version):
    versions = await open_request("""
                                select version.raw_text from version
                                where version.version_name = :version
                                
                                """,
                                  params = {"version":version},
                                  fetch_as_dict=True)
    text = versions[0]['raw_text'].replace('\n',' ')
    text = ''.join([i for i in text if i.isalpha() or i == ' '])
    text = [x for x in text.split(' ') if len(x)>0]
    text = [x.lower() for x in text]
    words = set(text)
    words_to_freq = {x:text.count(x) for x in words}
    words_to_freq = dict(sorted(words_to_freq.items(), key=lambda item: item[1], reverse=True))
    return words_to_freq

