# -*- coding: utf-8 -*-
"""

Class for triplets.

Authors:
    Fangzhou Li - fzli@ucdavis.edu

"""

from ast import literal_eval

import pandas as pd


class Triplets:
    """Class definition for Triplets for the food atlas.

    Args:
        path_triplets (str): Path to the triplets file.

    Attributes:
        COLUMNS (list): List of column names for the triplets file.
        FAID_PREFIX (str): Prefix for the foodatlas_id.

    """

    COLUMNS = [
        "foodatlas_id",
        "head_id",
        "relationship_id",
        "tail_id",
        "metadata_ids",
    ]
    FAID_PREFIX = "t"

    def __init__(
        self,
        path_triplets: str,
    ):
        self.path_triplets = path_triplets

        self._load()

    def _load(self):
        """Helper for loading the triplets."""
        self._triplets = pd.read_csv(
            self.path_triplets,
            sep="\t",
            converters={"metadata_ids": literal_eval},
        ).set_index("foodatlas_id")

        # Record the current maximum foodatlas_id.
        tid = self._triplets.index.str.slice(1).astype(int).max()
        self._curr_tid = tid + 1 if pd.notna(tid) else 1

        # Create a hash table for existing triplets.
        self._ht_t2m = {}
        self._triplets.apply(
            lambda row: self._ht_t2m.update(
                {
                    f"{row['head_id']}_{row['relationship_id']}_{row['tail_id']}": row[
                        "metadata_ids"
                    ]
                }
            ),
            axis=1,
        )

    def _save(self, path_output_dir: str):
        """Helper for saving the triplets.

        Args:
            path_output_dir (str): Path to the output directory.

        """
        self._triplets.to_csv(
            f"{path_output_dir}/triplets.tsv",
            sep="\t",
        )

    # def get(
    #     self,
    #     triplet_ids: str|list[str] = None,
    # ) -> pd.DataFrame:
    #     """Get triplets.

    #     Args:
    #         triplet_ids (str|list[str], optional): Triplet IDs to retrieve.
    #         Defaults to None, which returns all triplets.

    #     Returns:
    #         pd.DataFrame: Triplets.

    #     """
    #     if triplet_ids is None:
    #         return self._triplets

    #     if isinstance(triplet_ids, str):
    #         triplet_ids = [triplet_ids]

    #     return self._triplets[
    #         self._triplets['foodatlas_id'].isin(triplet_ids)
    #     ]

    def create(
        self,
        metadata: pd.DataFrame,
    ):
        """Create new triplet entries.

        Args:
            metadata (pd.DataFrame): Metadata for the new triplets.

        Returns:
            pd.Series: Added triplets.

        """
        head_ids = metadata["head_id"].tolist()
        relationship_ids = metadata["relationship_id"].tolist()
        tail_ids = metadata["tail_id"].tolist()
        metadata_ids = metadata.index.tolist()

        triplets_new_rows = []
        for head_id, relationship_id, tail_id, metadata_id in zip(
            head_ids, relationship_ids, tail_ids, metadata_ids, strict=False
        ):
            if f"{head_id}_{relationship_id}_{tail_id}" in self._ht_t2m:
                self._ht_t2m[f"{head_id}_{relationship_id}_{tail_id}"] += [metadata_id]
                continue
            triplets_new_rows += [
                {
                    "foodatlas_id": f"{self.FAID_PREFIX}{self._curr_tid}",
                    "head_id": head_id,
                    "relationship_id": relationship_id,
                    "tail_id": tail_id,
                    "metadata_ids": None,
                }
            ]
            self._curr_tid += 1
            self._ht_t2m[f"{head_id}_{relationship_id}_{tail_id}"] = [metadata_id]

        triplets_new = pd.DataFrame(triplets_new_rows).set_index("foodatlas_id")

        self._triplets = pd.concat([self._triplets, triplets_new])
        self._triplets["metadata_ids"] = self._triplets.apply(
            lambda row: list(
                set(
                    self._ht_t2m[
                        f"{row['head_id']}_{row['relationship_id']}_{row['tail_id']}"
                    ]
                )
            ),
            axis=1,
        )

        return triplets_new.apply(
            lambda row: self._ht_t2m[
                f"{row['head_id']}_{row['relationship_id']}_{row['tail_id']}"
            ],
            axis=1,
        )

    def get_by_relationship_id(
        self,
        relationship_id: str,
    ) -> pd.DataFrame:
        """Get triplets by relationship ID.

        Args:
            relationship_id (str): Relationship ID.

        Returns:
            pd.DataFrame: Triplets.

        """
        return self._triplets[
            self._triplets["relationship_id"] == relationship_id
        ].copy()
