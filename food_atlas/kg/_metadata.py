from ast import literal_eval

import pandas as pd


class Metadata:
    """
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
        """
        """
        self._metadata_contains = pd.read_csv(
            self.path_metadata_contains,
            sep='\t',
            converters={
                'reference': literal_eval,
                '_conc': lambda x: '' if pd.isna(x) else x,
                '_food_part': lambda x: '' if pd.isna(x) else x,
            },
        )

        mcid = self._metadata_contains['foodatlas_id'].str.slice(2).astype(int).max()
        self._curr_mcid = mcid + 1 if pd.notna(mcid) else 1

    def _save(self, path_output_dir):
        self._metadata_contains.to_csv(
            f"{path_output_dir}/metadata_contains.tsv",
            sep='\t',
            index=False,
        )

    # def _check_inputs(
    #     self,
    #     mdata: pd.DataFrame,
    # ):
    #     """
    #     """
    #     if set(mdata.columns.tolist()) != set(self.COLUMNS):
    #         raise ValueError(
    #             f'Columns in mdata must be {self.COLUMNS}, not {mdata.columns.tolist()}'
    #         )

    def create(
        self,
        metadata: pd.DataFrame,
    ):
        """
        """
        metadata = metadata.reset_index(drop=True)
        metadata['foodatlas_id'] \
            = self.FAID_PREFIX + (self._curr_mcid + metadata.index).astype(str)
        metadata = metadata[self.COLUMNS]

        self._curr_mcid += len(metadata)
        self._metadata_contains = pd.concat(
            [self._metadata_contains, metadata],
            ignore_index=True,
        )

        return metadata

    def update(self):
        pass