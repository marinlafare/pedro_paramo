# dissection_table.operations.umap_reduction.py

import asyncio
import umap
import numpy as np
from collections import OrderedDict
from dissection_table.operations.version_corpus import Corpus

def perform_umap_reduction(vectors_list,
                           n_components=3,
                           n_neighbors=15,
                           min_dist=0.1,
                           random_state=42):
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
    # if not vectors_list:
    #     print("Warning: Input list of vectors is empty. Returning empty list.")
    #     return []

    # Convert the list of vectors to a NumPy array
    #embeddings_array = np.array(vectors_list)

    # Initialize UMAP reducer
    reducer = umap.UMAP(
        n_components=n_components,
        n_neighbors=n_neighbors,
        min_dist=min_dist,
        random_state=random_state
    )

    # Fit and transform the embeddings
    reduced_embeddings = reducer.fit_transform(vectors_list)

    # Convert back to a list of lists for the return value
    return reduced_embeddings.tolist()