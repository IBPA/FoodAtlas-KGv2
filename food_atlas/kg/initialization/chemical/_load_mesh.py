from collections import OrderedDict

import pandas as pd


def load_mapper_name_to_mesh_id() -> pd.Series:
    """Load mapper from MeSH name to MeSH ID.

    Returns:
        pd.Series: Mapper from MeSH name to MeSH ID.

    """
    mesh_desc = pd.read_json(
        "outputs/data_processing/mesh_desc_cleaned.json",
        orient='records',
        lines=True,
    ).set_index('name')
    mesh_supp = pd.read_json(
        "outputs/data_processing/mesh_supp_cleaned.json",
        orient='records',
        lines=True,
    ).set_index('name')

    mapper_name_2_mesh_id = pd.concat([mesh_desc, mesh_supp])['id']

    return mapper_name_2_mesh_id


def load_mesh() -> pd.DataFrame:
    """Load MeSH terms.

    Returns:
        pd.DataFrame: MeSH terms.

    """
    meshd = pd.read_json(
        "outputs/data_processing/mesh_desc_cleaned.json",
        orient='records',
        lines=True,
    ).set_index('id')
    meshs = pd.read_json(
        "outputs/data_processing/mesh_supp_cleaned.json",
        orient='records',
        lines=True,
    ).set_index('id')

    mesh = pd.concat([meshd['synonyms'], meshs['synonyms']])
    mesh = mesh.apply(
        lambda names: list(OrderedDict.fromkeys(
            [name.lower() for name in names]).keys()
        )
    )

    return mesh
