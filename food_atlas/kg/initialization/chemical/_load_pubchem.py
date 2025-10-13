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
        sep="\t",
        names=["cid", "mesh_term", "mesh_term_alt"],
    ).set_index("cid")

    mapper_cid_to_mash_name["mesh_id"] = mapper_cid_to_mash_name["mesh_term"].map(
        mapper_name_2_mesh_id
    )
    mapper_cid_to_mash_id = mapper_cid_to_mash_name["mesh_id"]
    mapper_cid_to_mash_id = mapper_cid_to_mash_id.dropna()

    return mapper_cid_to_mash_id


def load_mapper_chebi_id_to_pubchem_cid() -> pd.DataFrame:
    """Load ChEBI ID to PubChem CID mapper.

    Returns:
        pd.DataFrame: Mapper.

    """
    chebi2cid = pd.read_parquet(
        "outputs/data_processing/pubchem-sid-map-small.parquet",
        columns=["registry_id", "cid"],
    )
    chebi2cid["chebi_id"] = chebi2cid["registry_id"].apply(
        lambda x: int(x.split(":")[-1])
    )
    chebi2cid["cid"] = chebi2cid["cid"].astype("Int64")
    chebi2cid = chebi2cid.dropna(subset=["cid"])
    chebi2cid = chebi2cid[["chebi_id", "cid"]]
    chebi2cid = chebi2cid.set_index("chebi_id")["cid"]

    return chebi2cid
