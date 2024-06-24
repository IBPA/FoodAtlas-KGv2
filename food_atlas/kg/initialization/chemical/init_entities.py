import pandas as pd
from pandarallel import pandarallel
from tqdm import tqdm

from ._load_chebi import load_mapper_name_2_chebi_id

tqdm.pandas()


def _get_entities_from_chebi():

    mapper = load_mapper_name_2_chebi_id()
    print(mapper)


if __name__ == '__main__':
    # We initialize chemical entities by using ChEBI, CDNO, and FDC Nutrients. The
    # primary ID is ChEBI, followed by CDNO and FDC.
    _get_entities_from_chebi()
    exit()
    mapper_name2chebi_id = load_mapper_name2chebi_id()
    mapper_name2chebi_id['_chebi_id'] = mapper_name2chebi_id['CHEBI_ID'].apply(
        lambda x: str(sorted(x))
    )

    def get_new_row(group):
        return pd.Series({
            'CHEBI_ID': group['CHEBI_ID'].values[0],
            'NAME': group['NAME'].tolist(),
        })
    chebi2names = mapper_name2chebi_id.groupby('_chebi_id').progress_apply(get_new_row)\
        .reset_index(drop=True)
    chebi2names = chebi2names.sort_values('CHEBI_ID', key=lambda x: len(x))

    print(chebi2names)