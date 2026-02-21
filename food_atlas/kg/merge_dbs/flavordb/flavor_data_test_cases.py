import ast

import pandas as pd


def heads_tails_exist(entities, triplets):
    assert (
        entities[
            entities["foodatlas_id"].isin(
                triplets[triplets["relationship_id"] == "r5"]["head_id"]
            )
        ]["entity_type"]
        == "chemical"
    ).all()
    assert (
        entities[
            entities["foodatlas_id"].isin(
                triplets[triplets["relationship_id"] == "r5"]["tail_id"]
            )
        ]["entity_type"]
        == "flavor"
    ).all()


def meta_data_exists(entities, triplets, meta):
    meta_ids = triplets[triplets["relationship_id"] == "r5"]["metadata_ids"].apply(
        lambda x: ast.literal_eval(x)[0]
    )
    assert meta_ids.isin(meta["foodatlas_id"]).all()


entities_no_flavor = pd.read_csv("./d_entities.tsv", delimiter="\t")
triplets_no_flavor = pd.read_csv("./d_triplets.tsv", delimiter="\t")

entities_with_flavor = pd.read_csv("./entities_disease_and_flavor.tsv", delimiter="\t")
triplets_with_flavor = pd.read_csv("./triplets_disease_and_flavor.tsv", delimiter="\t")
flavor_metadata = pd.read_csv("./flavor_metadata.tsv", delimiter="\t")

heads_tails_exist(entities_with_flavor, triplets_with_flavor)
meta_data_exists(entities_with_flavor, triplets_with_flavor, flavor_metadata)
