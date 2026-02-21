import os
from ast import literal_eval
from itertools import chain

import pandas as pd
from pandarallel import pandarallel

pandarallel.initialize(progress_bar=True)


def load_mesh() -> pd.DataFrame:
    """ """
    if os.path.exists("outputs/postprocessing/mesh_categories.json"):
        return pd.read_json("outputs/postprocessing/mesh_categories.json", lines=True)

    # Data cleaning.

    ## Process the description MeSH terms.
    meshd = pd.read_json("outputs/data_processing/mesh_desc_cleaned.json", lines=True)

    ### Formatting everything to a list of strings.
    meshd["tree_numbers"] = meshd["tree_numbers"].apply(
        lambda x: [x] if isinstance(x, str) else x
    )
    ### Remove tree numbers that are not chemical/drug related.
    meshd["tree_numbers"] = meshd["tree_numbers"].apply(
        lambda x: [y for y in x if y.startswith("D")]
    )
    ### Drop rows with no tree numbers.
    meshd = meshd[meshd["tree_numbers"].apply(len) > 0].copy()

    ### Get primary and secondary tree numbers.
    def get_category_tree_numbers(x):
        primary_tree_numbers = set()
        secondary_tree_numbers = set()
        for xx in x:
            primary_tree_number = xx.split(".")[0]
            primary_tree_numbers.add(primary_tree_number)
            try:
                secondary_tree_number = xx.split(".")[1]
                secondary_tree_numbers.add(
                    f"{primary_tree_number}.{secondary_tree_number}"
                )
            except IndexError:
                pass

        return sorted(list(primary_tree_numbers)), sorted(list(secondary_tree_numbers))

    meshd[["primary_tree_numbers", "secondary_tree_numbers"]] = (
        meshd["tree_numbers"].apply(get_category_tree_numbers).apply(pd.Series)
    )

    meshd = meshd.set_index("id")

    ## Process the supplementary MeSH terms.
    meshs = pd.read_json("outputs/data_processing/mesh_supp_cleaned.json", lines=True)

    ### Remove the prefices from the mapped_to column. '*' indicates a exact match.
    meshs["mapped_to"] = meshs["mapped_to"].apply(
        lambda x: [xx[1:] if xx.startswith("*") else xx for xx in x]
    )

    ### Remove supplementary terms that are not chemical/drug related.
    meshd_ids = set(meshd.index.tolist())
    meshs["mapped_to"] = meshs["mapped_to"].apply(
        lambda x: [y for y in x if y in meshd_ids]
    )

    ### Drop rows with no mapped_to.
    meshs = meshs[meshs["mapped_to"].apply(len) > 0].copy()

    def retrieve_tree_numbers(mapped_to):
        flatten_primary = set(
            chain.from_iterable(meshd.loc[mapped_to, "primary_tree_numbers"])
        )
        flatten_secondary = set(
            chain.from_iterable(meshd.loc[mapped_to, "secondary_tree_numbers"])
        )

        return sorted(list(flatten_primary)), sorted(list(flatten_secondary))

    meshs[["primary_tree_numbers", "secondary_tree_numbers"]] = (
        meshs["mapped_to"].parallel_apply(retrieve_tree_numbers).apply(pd.Series)
    )

    # Map the cleaned MeSH terms to the categories.
    meshd = meshd.reset_index()
    mesh = pd.concat([meshd, meshs])
    primary_tree_numbers = meshd["primary_tree_numbers"].explode().unique().tolist()
    secondary_tree_numbers = (
        meshd["secondary_tree_numbers"].explode().dropna().unique().tolist()
    )
    tree_number_to_mesh_id = (
        meshd.explode("tree_numbers")
        .set_index("tree_numbers")
        .loc[primary_tree_numbers + secondary_tree_numbers, ["id", "name"]]
    )

    def map_tree_number_to_categories(row):
        primary_tree_numbers = row["primary_tree_numbers"]
        secondary_tree_numbers = row["secondary_tree_numbers"]

        primary_category = tree_number_to_mesh_id.loc[
            primary_tree_numbers, "name"
        ].tolist()
        secondary_category = tree_number_to_mesh_id.loc[
            secondary_tree_numbers, "name"
        ].tolist()

        return primary_category, secondary_category

    mesh[["primary_category", "secondary_category"]] = mesh.parallel_apply(
        map_tree_number_to_categories, axis=1
    ).apply(pd.Series)

    mesh.to_json(
        "outputs/postprocessing/mesh_categories.json",
        orient="records",
        lines=True,
    )

    return mesh


if __name__ == "__main__":
    mesh = load_mesh().set_index("id")
    print(mesh)

    # Load checmicals.
    entities = pd.read_csv(
        "outputs/kg/entities.tsv", sep="\t", converters={"external_ids": literal_eval}
    ).query("entity_type == 'chemical'")

    entities["mesh_id"] = entities["external_ids"].apply(lambda x: x.get("mesh", []))

    def map_mesh_id_to_categories(mesh_id):
        # Each entity has at most one MeSH ID.
        if not mesh_id or mesh_id[0] not in mesh.index:
            return [], [], [], []

        mesh_id = mesh_id[0]
        return (
            mesh.loc[mesh_id, "primary_tree_numbers"],
            mesh.loc[mesh_id, "secondary_tree_numbers"],
            mesh.loc[mesh_id, "primary_category"],
            mesh.loc[mesh_id, "secondary_category"],
        )

    entities[
        ["mesh_tree_numbers_lvl1", "mesh_tree_numbers_lvl2", "mesh_lvl1", "mesh_lvl2"]
    ] = entities["mesh_id"].parallel_apply(map_mesh_id_to_categories).apply(pd.Series)

    entities[
        [
            "foodatlas_id",
            "mesh_tree_numbers_lvl1",
            "mesh_tree_numbers_lvl2",
            "mesh_lvl1",
            "mesh_lvl2",
        ]
    ].to_csv(
        "chemical_groups_mesh_for_trevor.tsv",
        sep="\t",
        index=False,
    )
