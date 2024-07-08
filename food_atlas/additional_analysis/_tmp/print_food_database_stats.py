from ast import literal_eval

import pandas as pd
from tqdm import tqdm

tqdm.pandas()


def print_foodb_stats():
    # Exclude `citatin_type` of 'PREDICTED'. This means PATHBANK and HMDB are ignored.
    content = pd.read_parquet("data/FooDB/Content.parquet", engine='pyarrow')
    content = content.query("export == 1")
    print("Original")
    print(f"# asso.     : {len(content)}")
    print(f"# triplets  : {len(content.groupby(['source_id', 'source_type', 'food_id'])['orig_content'].mean().reset_index())}")
    print(f"# food      : {content['food_id'].nunique()}")
    print(f"# chemical  : {content['source_id'].nunique()}")
    print()

    # Remove PRETICTED associations.
    content = content.query("citation_type != 'PREDICTED'")
    print("After removing PREDICTED")
    print(f"# asso.     : {len(content)}")
    print(f"# triplets  : {len(content.groupby(['source_id', 'source_type', 'food_id'])['orig_content'].mean().reset_index())}")
    print(f"# food      : {content['food_id'].nunique()}")
    print(f"# chemical  : {content['source_id'].nunique()}")
    print()

    # Remove UNKNOWN associations.
    content = content.query("citation_type != 'UNKNOWN'")
    print("After removing UNKNOWN")
    print(f"# asso.     : {len(content)}")
    print(f"# triplets  : {len(content.groupby(['source_id', 'source_type', 'food_id'])['orig_content'].mean().reset_index())}")
    print(f"# food      : {content['food_id'].nunique()}")
    print(f"# chemical  : {content['source_id'].nunique()}")
    print()

    # Count concentrations.
    content = content.dropna(subset=['orig_content'])
    content = content[content['orig_content'] > 0]
    print("With non-zero concentration information")
    print(f"# asso.     : {len(content)}")
    print(f"# triplets  : {len(content.groupby(['source_id', 'source_type', 'food_id'])['orig_content'].mean().reset_index())}")
    print(f"# food      : {content['food_id'].nunique()}")
    print(f"# chemical  : {content['source_id'].nunique()}")
    print()


def print_foodatlas_stats():
    entities = pd.read_csv(
        "outputs/kg/entities.tsv", sep='\t', converters={'external_ids': literal_eval}
    ).set_index('foodatlas_id')
    triplets = pd.read_csv(
        "outputs/kg/triplets.tsv", sep='\t', converters={'metadata_ids': literal_eval}
    )
    metadata = pd.read_csv(
        "outputs/kg/metadata_contains.tsv", sep='\t'
    ).set_index('foodatlas_id')

    triplets = triplets[triplets['relationship_id'] == 'r1']

    def append_has_conc(row):
        return not pd.isna(metadata.loc[row['metadata_ids'], 'conc_unit']).all()


    triplets['has_conc'] = triplets.progress_apply(append_has_conc, axis=1)
    print(triplets)
    print(f"# food: {triplets['head_id'].nunique()}")
    print(f"# chemical: {triplets['tail_id'].nunique()}")
    print(f"# triplets with conc: {triplets['has_conc'].sum()}")

    entities_chemical = entities.loc[triplets['tail_id'].unique()]
    entities_chemical_no = entities_chemical[entities_chemical['external_ids'] != {}]
    print(entities_chemical)
    print(entities_chemical_no)


if __name__ == '__main__':
    # print_foodb_stats()
    print_foodatlas_stats()
