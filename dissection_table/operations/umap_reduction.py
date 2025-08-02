# umap_reduction.py

import asyncio
import umap
import numpy as np
from collections import OrderedDict
from dissection_table.operations.version_corpus import Corpus

async def demonstrate_corpus_umap(version_name="spanish_1", n_components=3):
    """
    Demonstrates how to use the Corpus class to fetch embeddings and apply UMAP reduction.

    Args:
        version_name (str): The name of the corpus version to process.
        n_components (int): The target dimensionality for UMAP.

    Returns:
        OrderedDict or str: An OrderedDict of reduced embeddings (n_paragraph: reduced_vector)
                            or an error message if the corpus version is not found.
    """
    print(f"--- Demonstrating UMAP reduction for Corpus version: '{version_name}' ---")
    
    # 1. Create an instance of the Corpus class.
    #    This will internally fetch the raw text data for the version.
    corpus_instance = await Corpus.create(version_name)
    
    # 2. Call the asynchronous method to get UMAP-reduced embeddings.
    #    This method fetches all embeddings from the DB and then applies UMAP.
    reduced_embeddings_dict = await corpus_instance.get_reduced_embeddings_umap(n_components=n_components)
    
    if isinstance(reduced_embeddings_dict, str):
        print(f"Error: {reduced_embeddings_dict}")
        return reduced_embeddings_dict
    else:
        print(f"UMAP reduction successful for '{version_name}' to {n_components} dimensions.")
        print(f"Number of reduced embeddings: {len(reduced_embeddings_dict)}")
        print("\nFirst 5 reduced embeddings (n_paragraph: [vector]):")
        for i, (n_para, reduced_vec) in enumerate(reduced_embeddings_dict.items()):
            if i >= 5:
                break
            print(f"  Paragraph {n_para}: {reduced_vec}")
        return reduced_embeddings_dict

# --- Option 2: A standalone function to process any list of vectors ---

def perform_umap_reduction(vectors_list, n_components=2, n_neighbors=15, min_dist=0.1, random_state=42):
    """
    Performs UMAP dimensionality reduction on a given list of vectors.

    Args:
        vectors_list (list of lists/arrays): The input list of high-dimensional vectors.
        n_components (int): The dimension of the space to embed into.
        n_neighbors (int): The size of local neighborhood.
        min_dist (float): The effective minimum distance between embedded points.
        random_state (int): Seed for random number generator for reproducibility.

    Returns:
        list of lists: A list of the UMAP-reduced vectors.
    """
    if not vectors_list:
        print("Warning: Input list of vectors is empty. Returning empty list.")
        return []

    # Convert the list of vectors to a NumPy array
    embeddings_array = np.array(vectors_list)

    # Initialize UMAP reducer
    reducer = umap.UMAP(
        n_components=n_components,
        n_neighbors=n_neighbors,
        min_dist=min_dist,
        random_state=random_state
    )

    # Fit and transform the embeddings
    reduced_embeddings = reducer.fit_transform(embeddings_array)

    # Convert back to a list of lists for the return value
    return reduced_embeddings.tolist()