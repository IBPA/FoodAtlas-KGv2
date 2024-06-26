from itertools import chain

import pandas as pd

from ._load_mesh import load_mapper_name_to_mesh_id


def load_mapper_pubchem_cid_to_mesh_id() -> pd.Series:
    """Load mapper from PubChem CID to MeSH ID.

    Returns:
        pd.Series: Mapper from PubChem CID to MeSH ID.

    """
    mapper_name_2_mesh_id = load_mapper_name_to_mesh_id()

    mapper_cid_to_mash_name = pd.read_csv(
        "data/PubChem/CID-MeSH.txt",
        sep='\t',
        names=['cid', 'mesh_term', 'mesh_term_alt'],
    ).set_index('cid')

    mapper_cid_to_mash_name['mesh_id'] = mapper_cid_to_mash_name['mesh_term'].map(
        mapper_name_2_mesh_id
    )
    mapper_cid_to_mash_id = mapper_cid_to_mash_name['mesh_id']
    mapper_cid_to_mash_id = mapper_cid_to_mash_id.dropna()

    return mapper_cid_to_mash_id


# def load_mapper_mention_to_pubchem_cid() -> pd.DataFrame:
#     """Load mapper from mention to PubChem CID.

#     Returns:
#         pd.DataFrame: Mapper from mention to PubChem CID and MeSH ID.

#     """
#     mapper_cid_2_mesh_id = load_mapper_pubchem_cid_to_mesh_id()
#     mapper_mention_2_cid = pd.read_csv(
#         "data/PubChem/mention_to_cid.txt",
#         sep='\t',
#         header=None,
#         names=['mention', 'cid'],
#         dtype={'cid': 'Int64'},
#     )
#     mapper_cid_2_chebi_id = pd.read_csv(
#         "data/PubChem/cid_to_chebi.txt",
#         sep='\t',
#         header=None,
#         names=['cid', 'chebi_id'],
#         dtype={'cid': 'Int64'},
#     )
#     mapper_cid_2_chebi_id = mapper_cid_2_chebi_id.dropna(subset=['chebi_id'])
#     mapper_cid_2_chebi_id = mapper_cid_2_chebi_id.groupby('cid')['chebi_id'].apply(list)
#     mapper_cid_2_chebi_id = mapper_cid_2_chebi_id.apply(
#         lambda x: [int(xx.split(':')[-1]) for xx in x]
#     )
#     mapper_mention_2_cid['mesh_id'] = mapper_mention_2_cid['cid'].map(
#         mapper_cid_2_mesh_id
#     )
#     mapper_mention_2_cid['chebi_id'] = mapper_mention_2_cid['cid'].map(
#         mapper_cid_2_chebi_id
#     )
#     mapper_mention_2_cid = mapper_mention_2_cid.dropna(subset=['cid'])

#     def group_ids(group):
#         mention = group['mention'].iloc[0]
#         cid = group['cid'].tolist()
#         mesh_id = group['mesh_id'].dropna().unique().tolist()
#         chebi_id = list(set(chain(*group['chebi_id'].dropna().tolist())))

#         result = {
#             'mention': mention,
#             'cid': cid,
#             'mesh_id': mesh_id,
#             'chebi_id': chebi_id,
#         }
#         result = pd.DataFrame([result])

#         return result

#     mapper_mention_2_cid \
#         = mapper_mention_2_cid.groupby('mention').apply(group_ids).set_index('mention')

#     return mapper_mention_2_cid


def load_mapper_chebi_id_to_pubchem_cid() -> pd.DataFrame:
    """Load ChEBI ID to PubChem CID mapper.

    Returns:
        pd.DataFrame: Mapper.

    """
    chebi2cid = pd.read_parquet(
        "outputs/data_processing/pubchem-sid-map-small.parquet",
        columns=['registry_id', 'cid'],
    )
    chebi2cid['chebi_id'] = chebi2cid['registry_id'].apply(
        lambda x: int(x.split(':')[-1])
    )
    chebi2cid['cid'] = chebi2cid['cid'].astype('Int64')
    chebi2cid = chebi2cid.dropna(subset=['cid'])
    chebi2cid = chebi2cid[['chebi_id', 'cid']]
    chebi2cid = chebi2cid.set_index('chebi_id')['cid']

    return chebi2cid
