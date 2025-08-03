import ebooklib
from ebooklib import epub
from bs4 import BeautifulSoup
from dissection_table.database.db_interface import DBInterface
from dissection_table.database.models import Version, Paragraph
import umap

import re
sources = dict()
sources["portuguese_1"] = epub.read_epub("dissection_table/database/sources/Pedro Páramo (Juan Rulfo [Rulfo, Juan])_portugues_(Z-Library).epub")
sources["spanish_1"] = epub.read_epub("dissection_table/database/sources/Pedro Paramo (Juan Rulfo)_espanol_(Z-Library).epub")
sources["spanish_2"] = epub.read_epub("dissection_table/database/sources/Pedro Páramo (Juan Rulfo)_first_espanol_(Z-Library).epub")
sources["english_1"] = epub.read_epub("dissection_table/database/sources/Pedro Paramo (Juan Rulfo)_peden_(Z-Library).epub")
sources["english_2"] = epub.read_epub("dissection_table/database/sources/Pedro Paramo (Juan Rulfo)_weatherford_ (Z-Library).epub")
sources["italian_1"] = epub.read_epub("dissection_table/database/sources/Pedro Páramo (Juan Rulfo)_italian_(Z-Library).epub")
sources["french_1"] = epub.read_epub("dissection_table/database/sources/Pedro Páramo (Juan Rulfo)_french_(Z-Library).epub")

def get_metadata(version:str = None):
    source = sources[version]
    source_items = list(source.get_items_of_type(ebooklib.ITEM_DOCUMENT))
    metadata = []

    for ind,item in enumerate(source_items):
        data_text = BeautifulSoup(item.get_body_content(), 'html.parser')
        paragraphs = [para.get_text() for para in data_text.find_all('p')]
        paragraphs = [x.replace('\xa0', '') for x in paragraphs]
        if len(paragraphs) == 0:
            continue
        if len(paragraphs) > 100:
            continue
        metadata += [f"########## ITEM {ind}"]
        metadata += paragraphs
    return '\n'.join([x for x in metadata])

spanish_1_hard_coded_data = {}
spanish_1_hard_coded_data["version_data"] = "NO_METADATA"
spanish_1_hard_coded_data["author"] = "Juan Rulfo"
spanish_1_hard_coded_data["year"] = 1955
spanish_1_hard_coded_data["editorial"] = "www.elejandria.com"
spanish_1_hard_coded_data["ISBN"] = None

spanish_2_hard_coded_data = {}
spanish_2_hard_coded_data["version_data"] = get_metadata(version = "spanish_2")
spanish_2_hard_coded_data["author"] = "Juan Rulfo"
spanish_2_hard_coded_data["year"] = 2011
spanish_2_hard_coded_data["editorial"] = "feather"
spanish_2_hard_coded_data["ISBN"] = None

portugues_1_hard_coded_data = {}
portugues_1_hard_coded_data["version_data"] = get_metadata(version = "portuguese_1")
portugues_1_hard_coded_data["author"] = "Eric Nepomuceno"
portugues_1_hard_coded_data["year"] = 2009
portugues_1_hard_coded_data["editorial"] = "Edições BestBolso"
portugues_1_hard_coded_data["ISBN"] = 9788577991167

english_1_hard_coded_data = {}
english_1_hard_coded_data["version_data"] = get_metadata(version = "english_1")
english_1_hard_coded_data["author"] = "Margaret Sayers Peden"
english_1_hard_coded_data["year"] = 1994
english_1_hard_coded_data["editorial"] = "Grove Press"
english_1_hard_coded_data["ISBN"] = 9780802133908

english_2_hard_coded_data = {}
english_2_hard_coded_data["version_data"] = get_metadata(version = "english_2")
english_2_hard_coded_data["author"] = "Douglas J. Weatherford"
english_2_hard_coded_data["year"] = 2023
english_2_hard_coded_data["editorial"] = "Grove Press"
english_2_hard_coded_data["ISBN"] = 9780802160935

italian_1_hard_coded_data = {}
italian_1_hard_coded_data["version_data"] = get_metadata(version = "italian_1")
italian_1_hard_coded_data["author"] = "Paolo Collo"
italian_1_hard_coded_data["year"] = 2014
italian_1_hard_coded_data["editorial"] = "Giulio Einaudi editore s.p.a."
italian_1_hard_coded_data["ISBN"] = 9788858440247

french_1_hard_coded_data = {}
french_1_hard_coded_data["version_data"] = get_metadata(version = "french_1")
french_1_hard_coded_data["author"] = "Gabriel Iaculli"
french_1_hard_coded_data["year"] = 2005
french_1_hard_coded_data["editorial"] = "Éditions Gallimard"
french_1_hard_coded_data["ISBN"] = 9782070379538


versions_data = dict()
versions_data["spanish_1"] = spanish_1_hard_coded_data
versions_data["spanish_2"] = spanish_2_hard_coded_data
versions_data["portuguese_1"] = portugues_1_hard_coded_data
versions_data["english_1"] = english_1_hard_coded_data
versions_data["english_2"] = english_2_hard_coded_data
versions_data["italian_1"] = italian_1_hard_coded_data
versions_data["french_1"] = french_1_hard_coded_data



def get_raw_text(source: dict= {}):
    if source == "portuguese_1":
        source = sources["portuguese_1"]
        whole_text = list(source.get_items_of_type(ebooklib.ITEM_DOCUMENT))[6]
        whole_text = BeautifulSoup(whole_text.get_body_content(), 'html.parser')
        paragraphs = [para.get_text() for para in whole_text.find_all('p')][:-2]
        paragraphs = [x.replace('\xa0', '') for x in paragraphs]
        return '\n'.join([x for x in paragraphs])
    elif source == "spanish_1":
        source = sources["spanish_1"]
        whole_text = list(source.get_items_of_type(ebooklib.ITEM_DOCUMENT))[0]
        whole_text = BeautifulSoup(whole_text.get_body_content(), 'html.parser')
        paragraphs = [para.get_text() for para in whole_text.find_all('p')][:-3]
        paragraphs = [x.replace('\xa0', '') for x in paragraphs]
        return '\n'.join([x for x in paragraphs])
    elif source == "spanish_2":
        source = sources["spanish_2"]
        whole_text = list(source.get_items_of_type(ebooklib.ITEM_DOCUMENT))[0]
        whole_text = BeautifulSoup(whole_text.get_body_content(), 'html.parser')
        paragraphs = [para.get_text() for para in whole_text.find_all('p')]
        paragraphs = [x.replace('\xa0', '') for x in paragraphs]
        return '\n'.join([x for x in paragraphs])
    elif source == "english_1":
        source = sources["english_1"]
        whole_text = list(source.get_items_of_type(ebooklib.ITEM_DOCUMENT))[1]
        whole_text = BeautifulSoup(whole_text.get_body_content(), 'html.parser')
        paragraphs = [para.get_text() for para in whole_text.find_all('p')]
        paragraphs = [x.replace('\xa0', '') for x in paragraphs]
        return '\n'.join([x for x in paragraphs[:-1]])
    elif source == "english_2":
        source = sources["english_2"]
        whole_text = list(source.get_items_of_type(ebooklib.ITEM_DOCUMENT))[6]
        whole_text = BeautifulSoup(whole_text.get_body_content(), 'html.parser')
        paragraphs = [para.get_text() for para in whole_text.find_all('p')]
        paragraphs = [x.replace('\xa0', '') for x in paragraphs]
        return '\n'.join([x for x in paragraphs])
    elif source == "italian_1":
        source = sources["italian_1"]
        whole_text = list(source.get_items_of_type(ebooklib.ITEM_DOCUMENT))[6]
        whole_text = BeautifulSoup(whole_text.get_body_content(), 'html.parser')
        paragraphs = [para.get_text() for para in whole_text.find_all('p')]
        paragraphs = [x.replace('\xa0', '') for x in paragraphs]
        return '\n'.join([x for x in paragraphs])
    elif source == "french_1":
        source = sources["french_1"]
        general = list(source.get_items_of_type(ebooklib.ITEM_DOCUMENT))
        a = BeautifulSoup(general[3].get_body_content(), 'html.parser')
        b = BeautifulSoup(general[4].get_body_content(), 'html.parser')
        b = [para.get_text() for para in b.find_all('p')]
        a = [para.get_text() for para in a.find_all('p')]
        complete = a[4:] + b
        complete = [x.replace('\xa0', '') for x in complete]
        return '\n'.join([x for x in complete])
    else:
        print(f'VERSION :: {source} :: not found, returning original: "spanish_1"')
        return get_raw_text("spanish_1")
import unicodedata

def clean_line(string: str = None) -> str:
    """
    Cleans a string by removing all non-alphabetic characters (preserving
    letters with diacritics) and converting all alphabetic characters to lowercase.

    Args:
        string (str): The input string to clean. If None, returns an empty string.

    Returns:
        str: The cleaned string.
    """
    if string is None:
        return ""
    
    # Use a list comprehension to filter characters.
    # unicodedata.category(char) returns the general category of the character.
    # 'L' category includes all letters (uppercase, lowercase, titlecase, modifier, other).
    # This correctly handles accented characters like 'á', 'ü', 'ñ'.
    cleaned_chars = [char for char in string if unicodedata.category(char).startswith('L')]
    
    # Join the filtered characters back into a string
    cleaned_string = "".join(cleaned_chars)
    
    # Convert all characters to lowercase (Python's .lower() handles Unicode correctly)
    cleaned_string = cleaned_string.lower()
    
    return cleaned_string


def version(source:str = None):
    try:
        hard_coded_data = versions_data[source]
    except:
        hard_coded_data = versions_data["spanish_1"]
    hard_coded_data["raw_text"] = get_raw_text(source)
    hard_coded_data["version_name"] = source
    words = hard_coded_data["raw_text"].replace('\n',' ').split(' ')
    words = [x for x in words if len(x)>0]
    words = [clean_line(x) for x in words]
    raw_words = '#'.join([x for x in words])
    
    word_set = set(words)
    n_words_len = len(words)
    
    hard_coded_data["n_words"] = n_words_len
    paragraphs = [x for x in hard_coded_data["raw_text"].split('\n') if len(x)>0]
    hard_coded_data['n_paragraphs'] = len(paragraphs)
    hard_coded_data['paragraphs'] = paragraphs
    hard_coded_data['words_set'] = '#'.join([x for x in word_set])
    hard_coded_data['raw_words'] = raw_words
    return hard_coded_data

async def feed_database():
    from langchain_ollama import OllamaEmbeddings
    
    print('ollama_and_umap_models_load')
    ollama_emb = OllamaEmbeddings(model="granite-embedding:278m")
    n_components = 3
    n_neighbors = 15
    min_dist = 0.1
    random_state = 42
    reducer = umap.UMAP(
            n_components=n_components,
            n_neighbors=n_neighbors,
            min_dist=min_dist,
            random_state=random_state
        )
    print('models_loaded')
    
    for current_version in sources.keys():
        print("####")
        print(current_version)
        print("####")
        
        version_interface = DBInterface(Version)
        paragraph_interface = DBInterface(Paragraph)
        data_version = version(current_version)
        text = data_version["raw_text"]
        paragraphs = data_version['paragraphs']
        embeddings_list = ollama_emb.embed_documents(paragraphs)
        reduced_embeddings = reducer.fit_transform(embeddings_list)
        
        version_paragraphs = []
        for ind, paragraph in enumerate(paragraphs):
            data = dict()
            data['version_name'] = data_version["version_name"]
            data["n_paragraph"] = ind
            data["text"] = paragraph
            data["n_words"] = len(paragraph.split(' '))
            data["embedding"] = embeddings_list[ind]
            data['umap'] = reduced_embeddings[ind]
            version_paragraphs.append(data)
        data_version.pop('paragraphs')
        await version_interface.create(data_version)
        await paragraph_interface.create_all(version_paragraphs)
    
        











    