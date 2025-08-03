# database.operations.frequencies.py
from dissection_table.database.ask_db import *
import re
from collections import OrderedDict, Counter 
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
    
async def get_word_freq_dict(version_name: str):
    """
    Retrieves the raw text for a given version, cleans its words,
    calculates word frequencies, and returns an OrderedDict
    sorted by frequency in descending order.

    Args:
        version_name (str): The name of the version to retrieve word data for.

    Returns:
        OrderedDict: A dictionary where keys are cleaned words (str) and
                     values are their frequencies (int), sorted by frequency
                     in descending order.
        str: An error message if the version doesn't exist or data is empty.
    """
    
    query = """
        SELECT version.raw_words FROM version
        WHERE version.version_name = :v_n;
    """
    
    # open_request is expected to return a list of tuples, e.g., [('full raw text string',)]
    data = await open_request(query, params={"v_n": version_name})
    
    if not data or not data[0] or not data[0][0]:
        return f"This version: {version_name} doesn't exist or has no raw text data."
    
    raw_text_string = data[0][0] # Get the full raw text string from the query result
    
    # Process the raw text:
    # 1. Replace newlines with spaces
    # 2. Split into words by space
    # 3. Filter out empty strings from splitting
    # 4. Clean each word using the clean_line function
    # 5. Filter out any words that become empty after cleaning
    
    # Initial split by space, then clean each word
    words_from_text = raw_text_string.replace('#', ' ').split(' ')
    processed_words = [word for word in words_from_text if word]
    
    if not processed_words:
        return f"No valid words found for version: {version_name} after cleaning."

    word_counts = Counter(processed_words)
    
    # Sort the items by frequency in descending order
    sorted_word_counts = sorted(word_counts.items(), key=lambda item: item[1], reverse=True)
    
    # Create an OrderedDict from the sorted list of (word, count) tuples
    word_freq_dict = OrderedDict(sorted_word_counts)
    
    return word_freq_dict

