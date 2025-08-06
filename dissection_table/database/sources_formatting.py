import ebooklib
from ebooklib import epub
from bs4 import BeautifulSoup
from dissection_table.database.db_interface import DBInterface
from dissection_table.database.models import Version, Paragraph, ParagraphSimilarity
import umap.umap_ as umap
import numpy as np
import joblib
import time
import re
from dissection_table.database.engine import init_db,get_db_session
from dissection_table.operations.version_corpus import Corpus
from scipy.spatial.distance import cdist
from sqlalchemy.ext.asyncio import AsyncSession
sources = dict()
sources["portuguese_1"] = epub.read_epub("dissection_table/database/sources/Pedro Páramo (Juan Rulfo [Rulfo, Juan])_portugues_(Z-Library).epub")
sources["spanish_1"] = epub.read_epub("dissection_table/database/sources/Pedro Paramo (Juan Rulfo)_espanol_(Z-Library).epub")
sources["spanish_2"] = epub.read_epub("dissection_table/database/sources/Pedro Páramo (Juan Rulfo)_first_espanol_(Z-Library).epub")
sources["english_1"] = epub.read_epub("dissection_table/database/sources/Pedro Paramo (Juan Rulfo)_peden_(Z-Library).epub")
sources["english_2"] = epub.read_epub("dissection_table/database/sources/Pedro Paramo (Juan Rulfo)_weatherford_ (Z-Library).epub")
sources["italian_1"] = epub.read_epub("dissection_table/database/sources/Pedro Páramo (Juan Rulfo)_italian_(Z-Library).epub")
sources["french_1"] = epub.read_epub("dissection_table/database/sources/Pedro Páramo (Juan Rulfo)_french_(Z-Library).epub")
sources["german_1"] = epub.read_epub("dissection_table/database/sources/Pedro Pâramo (Rulfo Juan)_german_(Z-Library).epub")
sources["turkish_1"] = epub.read_epub("dissection_table/database/sources/Pedro Paramo (Rulfo Juan)_turkish_ (Z-Library).epub")

def get_german_1_metadata(version:str = None):
    source = sources[version]
    source_items = list(source.get_items_of_type(ebooklib.ITEM_DOCUMENT))
    raw = BeautifulSoup(source_items[2].get_body_content(), 'html.parser')
    paragraphs = [para.get_text() for para in raw.find_all('p')]
    text = '\n'.join([x for x in paragraphs[44:-48]])
    meta_one =  "### META_ONE \n"+'\n'.join([x for x in paragraphs[:44]])
    meta_two =  "### META_TWO \n"+'\n'.join([x for x in paragraphs[-48:-21]])
    meta_three ="### META_THREE \n"+'\n'.join([x for x in paragraphs[-21:]])
    metadata = meta_one + meta_two + meta_three
    return text, metadata
    
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

german_1_hard_coded_data = {}
_, else_data = get_german_1_metadata(version = "german_1")
german_1_hard_coded_data["version_data"] = else_data
german_1_hard_coded_data["author"] = "Dagmar Ploetz"
german_1_hard_coded_data["year"] = 2008
german_1_hard_coded_data["editorial"] = "Carl Hanser Verlag Munich"
german_1_hard_coded_data["ISBN"] = 9783446230668

turkish_1_hard_coded_data = {}
turkish_1_hard_coded_data["version_data"] = 'NO_METADATA'
turkish_1_hard_coded_data["author"] = "Tomris Uyar"
turkish_1_hard_coded_data["year"] = 1983
turkish_1_hard_coded_data["editorial"] = "Can Yayinlari"
turkish_1_hard_coded_data["ISBN"] = None


versions_data = dict()
versions_data["spanish_1"] = spanish_1_hard_coded_data
versions_data["spanish_2"] = spanish_2_hard_coded_data
versions_data["portuguese_1"] = portugues_1_hard_coded_data
versions_data["english_1"] = english_1_hard_coded_data
versions_data["english_2"] = english_2_hard_coded_data
versions_data["italian_1"] = italian_1_hard_coded_data
versions_data["french_1"] = french_1_hard_coded_data
versions_data["german_1"] = german_1_hard_coded_data
versions_data["turkish_1"] = turkish_1_hard_coded_data

    



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
    elif source == "german_1":
        text,_ = get_german_1_metadata(version = "german_1")
        return text
    elif source == "turkish_1":
        source = sources["turkish_1"]
        source = list(source.get_items_of_type(ebooklib.ITEM_DOCUMENT))
        whole_text = BeautifulSoup(source[2].get_body_content(), 'html.parser')
        paragraphs = [para.get_text() for para in whole_text.find_all('p')][:-2]
        return '\n'.join([x for x in paragraphs])
    else:
        print(f'VERSION :: {source} :: not found, returning original: "spanish_1"')
        return "NO VERSION SORRY"

import unicodedata
def clean_line(string: str = None) -> str:
    apostrophes = {"'", "’", "`"}
    
    cleaned_chars = []
    for char in string:
        if unicodedata.category(char).startswith('L') or char in apostrophes:
            cleaned_chars.append(char)
    
    cleaned_string = "".join(cleaned_chars)
    
    cleaned_string = cleaned_string.lower()
    
    return cleaned_string


def get_version(source:str = None):
    try:
        hard_coded_data = versions_data[source]
    except:
        print(f"Sorcue: {source} doesn't exists")
        return
    
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
    
async def get_umap_model(n_components:int = 3,
                        n_neighbors: int = 15,
                        min_dist:float = 0.1,
                        random_state:int = 42,
                        ):
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
    return reducer
from sqlalchemy import insert

async def insert_paragraph_similarities(session: AsyncSession, similarity_data: dict):
    """
    Inserts similarity data from a nested dictionary into the database
    using a bulk insert approach with a DBInterface object.
    Explicitly generates an ID for each entry to avoid IntegrityError.

    Args:
        session (AsyncSession): The SQLAlchemy async session.
        similarity_data (dict): The nested dictionary with similarity results.
    """
    new_similarity_entries = []
    
    # Iterate through the top-level keys like 'portuguese_1#0'
    for source_key, target_data in similarity_data.items():
        # Split the source key to get the version name and paragraph number
        source_version_name, source_n_paragraph_str = source_key.split('#')
        source_n_paragraph = int(source_n_paragraph_str)
        
        # Iterate through the inner dictionary of target versions
        for target_version_name, target_n_paragraphs in target_data.items():
            # The list contains the ranks, so we enumerate to get the rank
            for rank, target_n_paragraph in enumerate(target_n_paragraphs):
                # Create a dictionary representing the data for one row
                similarity_entry = {
                    "source_version_name": source_version_name,
                    "source_n_paragraph": source_n_paragraph,
                    "target_version_name": target_version_name,
                    "target_n_paragraph": target_n_paragraph,
                    "rank": rank
                }
                new_similarity_entries.append(similarity_entry)
                
    
    # Use async with for transaction management and perform the bulk insert
    async with session.begin():
        await session.execute(
            insert(ParagraphSimilarity),
            new_similarity_entries
        )
async def paragraph_similarity():
    results = {}
    matrices = {}
    async with DBInterface(ParagraphSimilarity).get_session() as session:
        for source in sources.keys():
            matrix = await Corpus.create(session, source)
            matrices[source] = await matrix.all_umap()
        
    for source_name, source_matrix in matrices.items():
        for i, source_row in enumerate(source_matrix):
            row_key = f'{source_name}#{i}'
            results[row_key] = {}
            
            for target_name, target_matrix in matrices.items():
                if source_name == target_name:
                    continue
                distances = cdist(source_row.reshape(1, -1), target_matrix, 'cosine')[0]
                similarities = 1 - distances
                num_target_rows = target_matrix.shape[0]
                num_to_get = min(3, num_target_rows)
                top_3_indices = np.argsort(similarities)[-num_to_get:][::-1]
                
                results[row_key][target_name] = top_3_indices.tolist()

    return results
async def concat_embeddings(sources):
    index = []
    all_paragraphs_text = []
    
    for version_data, paragraphs_list_of_strings in sources:
        version_name = version_data['version_name']

        
        
        for ind, paragraph_text_string in enumerate(paragraphs_list_of_strings):
            index.append(f"{version_name}#{ind}")
            all_paragraphs_text.append(paragraph_text_string)
    
    print('ollama_start_embeddings')
    star_ollama_embeddings = time.time()
    from langchain_ollama import OllamaEmbeddings  
    ollama_emb = OllamaEmbeddings(model="granite-embedding:278m")
    embeddings = ollama_emb.embed_documents(all_paragraphs_text)
    print('ollama_end_embeddings')
    print(f'TIME ELAPSED FOR ALL EMBEDDINGS: {(time.time()-star_ollama_embeddings)/60}')
    print('umap_start_reducer')
    star_umap_embeddings = time.time()
    umap_reducer = await get_umap_model()
    umap_embeddings = umap_reducer.fit_transform(embeddings)
    print('umap_end_reducer')
    print(f'TIME ELAPSED FOR ALL EMBEDDINGS: {(time.time()-star_umap_embeddings)/60}')

    print('ALL PARAGRAPHS EMBEDDING START')
    start_all_embeddings = time.time()
    
    raw_texts_list = []
    for version_data, _ in sources:
        raw_texts_list.append(version_data['raw_text'])
    
    print('Generating embeddings for raw texts...')
    text_embeddings = ollama_emb.embed_documents(raw_texts_list)
    print(f'TIME ELAPSED FOR ALL PARAGRAPH_EMB: {(time.time()-start_all_embeddings)/60}')
    start_all_umap = time.time()
    print('Applying UMAP reduction to raw text embeddings...')
    all_umap_reducer = await get_umap_model(n_neighbors = 2)
    
    text_umap_embeddings = all_umap_reducer.fit_transform(text_embeddings)
    print(f'TIME ELAPSED FOR ALL UMAP: {(time.time()-start_all_umap)/60}')
    print('Assigning text embeddings and UMAP to version data...')
    for i, (version_data, _) in enumerate(sources):
        version_data['text_embedding'] = text_embeddings[i]
        version_data['text_umap'] = text_umap_embeddings[i].tolist()

    versions = [x[0] for x in sources]
    joblib.dump(umap_reducer,'dissection_table/database/sources/umap_reducer_8_languagues.pkl')
    return index, embeddings, umap_embeddings, all_paragraphs_text, versions


    
async def feed_database():
    packed_sources = []
    for current_version in sources.keys():
        
        version_interface = DBInterface(Version)
        paragraph_interface = DBInterface(Paragraph)
        
        data_version = get_version(current_version)
        
        
        # the repetition happens here
        text = data_version["raw_text"]
        paragraphs = data_version['paragraphs']
        clean_for_embedding = [
                                [clean_line(x).replace('  ',' ') for x in paragraphs[ind].split(' ')]
                                for ind in range(len(paragraphs))
                                ]
        clean_for_embedding = [' '.join([x for x in clean_for_embedding[ind]])
                                for ind in range(len(clean_for_embedding))]
        clean_for_embedding = [x[1:].replace('  ',' ') if x.startswith(' ') else x.replace('  ',' ') for x in clean_for_embedding]

        packed_sources.append((data_version, clean_for_embedding))
        
    index, embeddings, umap_embeddings, paragraphs, versions = await concat_embeddings(packed_sources)
    version_paragraphs = []
    for ind, index_data in enumerate(index):
        
        data = dict()
        data['version_name'] = index_data.split('#')[0]
        data["n_paragraph"] = int(index_data.split('#')[-1])
        data["text"] = paragraphs[ind]
        data["n_words"] = len([x for x in paragraphs[ind].split(' ') if len(x)>0])
        data["embedding"] = embeddings[ind]
        data['umap'] = umap_embeddings[ind]
        version_paragraphs.append(data)
    for version in versions:
        
        try:
            version.pop('paragraphs')
        except:
            print('%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%')
            print(version['version_name'])
            print('%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%')

    await version_interface.create_all(versions)
    await paragraph_interface.create_all(version_paragraphs)
    
    return "DONEEEEEEEEEEEEEEEEEEE"
    
async def create_similarity_data():
    similarity_data  = await paragraph_similarity()
    async with DBInterface(ParagraphSimilarity).get_session() as session:
        await insert_paragraph_similarities(session=session, similarity_data=similarity_data)











    