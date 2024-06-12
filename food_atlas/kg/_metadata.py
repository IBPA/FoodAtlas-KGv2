# -*- coding: utf-8 -*-
"""

Metadata Class.

Authors:
    Fangzhou Li - fzli@ucdavis.edu

"""
from ast import literal_eval

import pandas as pd


class Metadata:
    """Class definition for Metadata for the food atlas.

    Args:
        path_metadata_contains (str): Path to the metadata file for contains.

    Attributes:
        COLUMNS (list): List of column names for the metadata file.
        FAID_PREFIX (str): Prefix for the foodatlas_id.

    """

    COLUMNS = [
        'foodatlas_id',
        'conc_value',
        'conc_unit',
        'food_part',
        'food_processing',
        'source',
        'reference',
        'quality_score',
        '_food_name',
        '_chemical_name',
        '_conc',
        '_food_part',
    ]
    FAID_PREFIX = 'mc'

    def __init__(
        self,
        path_metadata_contains: str,
    ):
        self.path_metadata_contains = path_metadata_contains

        self._load()

    def _load(self):
        """Helper for loading the metadata.

        """
        self._metadata_contains = pd.read_csv(
            self.path_metadata_contains,
            sep='\t',
            converters={
                'reference': literal_eval,
                '_conc': lambda x: '' if pd.isna(x) else x,
                '_food_part': lambda x: '' if pd.isna(x) else x,
            },
        ).set_index('foodatlas_id')

        mcid = self._metadata_contains.index.str.slice(2).astype(int).max()
        self._curr_mcid = mcid + 1 if pd.notna(mcid) else 1

    def _save(self, path_output_dir: str):
        """Helper for saving the metadata.

        Args:
            path_output_dir (str): Path to the output directory.

        """
        self._metadata_contains.to_csv(
            f"{path_output_dir}/metadata_contains.tsv",
            sep='\t',
        )

    def create(
        self,
        metadata: pd.DataFrame,
    ):
        """Create new metadata entries.

        Args:
            metadata (pd.DataFrame): Metadata to be added.

        Returns:
            pd.DataFrame: Metadata added with foodatlas_id.

        """
        metadata = metadata.reset_index(drop=True)
        metadata['foodatlas_id'] \
            = self.FAID_PREFIX + (self._curr_mcid + metadata.index).astype(str)
        metadata = metadata[self.COLUMNS].set_index('foodatlas_id')

        self._curr_mcid += len(metadata)
        self._metadata_contains = pd.concat(
            [self._metadata_contains, metadata],
        )

        return metadata

    def update(self):
        pass

    def get(
        self,
        metadata_ids: list[str],
    ) -> pd.DataFrame:
        """Get metadata.

        Args:
            metadata_ids (list[str]): List of metadata ids.

        Returns:
            pd.DataFrame: Metadata.

        """
        return self._metadata_contains.loc[metadata_ids]
