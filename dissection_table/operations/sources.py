# database.operations.sources.py
from dissection_table.database.ask_db import *
async def get_versions_names():
    versions = await open_request("select version_name from version", fetch_as_dict=True)
    return versions
async def get_raw_text(version):
    
    data = await open_request("""
                                select raw_text from version
                                where version.version_name = :version_name""",
                               params = {"version_name":version},
                             fetch_as_dict = True)
    return data[0]
async def get_paragraphs(version):
    
    data = await open_request("""
                                 select n_paragraph, text from paragraph
                                 where version_name = :version_name""",
                              params = {"version_name": version},
                              fetch_as_dict = True)
    
    # Sort the list of dictionaries by n_paragraph before creating the new dictionary
    sorted_data = sorted(data, key=lambda x: x["n_paragraph"])
    
    # Create the dictionary from the sorted list
    result_dict = {x["n_paragraph"]: x["text"] for x in sorted_data}
    
    return result_dict
async def get_metadata(version):
    data = await open_request("""
                                select version_data from version
                                where version.version_name = :version_name""",
                               params = {"version_name":version},
                             fetch_as_dict = True)
    return data[0]
async def get_complete_version(version):
    data = await open_request("""
                                select * from version
                                where version.version_name = :version_name""",
                               params = {"version_name":version},
                             fetch_as_dict = True)
    return data[0]