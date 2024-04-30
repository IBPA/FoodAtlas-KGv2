# -*- coding: utf-8 -*-
"""Provides a utility function that given a list of sets, merge all the sets that have
overlapping elements.

Authors:
    Fangzhou Li - fzli@ucdavis.edu

"""
from tqdm import tqdm


def merge_sets(sets: list[list[str]]) -> list[str]:
    """Merge a list of sets with overlapping elements. Complexity: O(n^2 * m).

    Args:
        sets (list[list[str]]): The list of sets.

    Returns:
        list[str]: The merged set.

    """
    def get_islands(graph: dict[int, set[int]]) -> list[set[int]]:
        """Apply DFS to get the islands, i.e., connected nodes, in the graph.

        Args:
            graph (dict[int, set[int]]): The graph.

        Returns:
            list[set[int]]: The list of islands.

        """
        visited = set()
        islands = []

        def dfs(node: int, island: set):
            visited.add(node)
            island.add(node)
            for neighbor in graph[node]:
                if neighbor not in visited:
                    dfs(neighbor, island)

        for node in graph:
            if node not in visited:
                island = set()
                dfs(node, island)
                islands += [island]

        return islands

    # Each set represents a node. The edge represents the existence of overlapping
    # elements.
    graph = {i: set() for i in range(len(sets))}
    for i in tqdm(range(len(sets)), total=len(sets)):
        for j in range(i + 1, len(sets)):
            if sets[i] & sets[j]:
                graph[i].add(j)
                graph[j].add(i)

    islands = get_islands(graph)

    merged_sets = []
    for island in islands:
        merged_set = set()
        for node in island:
            merged_set |= sets[node]
        merged_sets += [merged_set]

    return merged_sets
