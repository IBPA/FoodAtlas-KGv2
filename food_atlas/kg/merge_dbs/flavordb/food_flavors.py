import ast

import pandas as pd
import torch
import kneed
from fuzzywuzzy import process

from ._load_hsdb import load_hsdb


# Takes in data scraped from FlavorDB and pubchem, outputs flavor metadata entries
def generate_flavor_metadata(flavordb_data_path, entities):
    flavordb_data = torch.load(flavordb_data_path)
    cid2odor, cid2taste = load_hsdb()

    # Extract metadata from data scraped from FlavorDB
    flavor_metadata = []
    for pc_id in flavordb_data:
        chemical = flavordb_data[pc_id]
        flavor_descriptors = chemical['flavor_profile'].split('@')
        flavor_descriptors.extend(chemical['flavor_profile'].split('@'))
        flavor_descriptors.extend(chemical['taste'].split('@'))
        flavor_descriptors.extend(chemical['odor'].split('@'))
        flavor_descriptors.extend(chemical['fooddb_flavor_profile'].split('@'))
        flavor_descriptors.extend(chemical['super_sweet'].split('@'))
        if chemical['bitter']:
            flavor_descriptors.append("bitter") # BitterDB is either 0/1
        for flavor in set(f.lower() for f in flavor_descriptors):
            if flavor and pc_id in entities['pc_id'].unique():
                flavor_metadata.append({
                    "foodatlas_id" : "mf",
                    "source" : "flavordb",
                    "reference" : {
                        'url': f"https://cosylab.iiitd.edu.in/flavordb/molecules_json?id={pc_id}"},
                    "_flavor" : flavor.lower(),
                    "_chemical" : entities[entities['pc_id'] == pc_id]['common_name'].iloc[0],
                    "_pubchem_id" : pc_id
                })
    flavor_metadata = pd.DataFrame(flavor_metadata)

    # Extract metadata from data scraped from Pubchem. Skip those already in FlavorDB
    # because FlavorDB also sourced from Pubchem.
    pc_ids_skip = set(flavor_metadata['_pubchem_id'].tolist())
    pubchem_flavor = []
    for mapper in [cid2odor, cid2taste]:
        for pc_id, data in mapper.items():
            if pc_id in pc_ids_skip:
                continue

            for ref in data:
                entry = {
                    'foodatlas_id' : "mf",
                    '_pubchem_id' : pc_id,
                    'source' : 'hsdb',
                    'reference' : {'url': ref['hsdb_url']},
                    '_flavor': ref['value'],
                }
                pubchem_flavor.append(entry)

    # for pc_id in pubchem_odor:
    #     data = pubchem_odor[pc_id]
    #     for ref in data:
    #         entry = {'foodatlas_id' : "mf", 
    #                  '_pubchem_id' : pc_id, 
    #                  'source' : 'PubChem', 
    #                  'reference' : ref.get('Reference', [f"https://pubchem.ncbi.nlm.nih.gov/compound/{pc_id}"])[0], 
    #                  '_flavor':ref['Value']['StringWithMarkup'][0]['String']}
    #         pubchem_flavor.append(entry)

    pubchem_flavor = pd.DataFrame(pubchem_flavor)
    pubchem_flavor = pubchem_flavor.merge(entities[['pc_id', 'common_name']], how='left', left_on='_pubchem_id', right_on='pc_id')
    pubchem_flavor.drop(['pc_id'], axis=1, inplace=True)
    pubchem_flavor.rename(columns={'common_name' : '_chemical'}, inplace=True)
    pubchem_flavor = pubchem_flavor[pubchem_flavor['_chemical'].notna()]

    # The pubchem flavor data is quite noisy, so I filter by using fuzzy
    # matching to match to our existing descriptors
    flavor_descriptors = flavor_metadata['_flavor'].explode().drop_duplicates().reset_index(drop=True)
    string_matches = pubchem_flavor['_flavor'].map(lambda x: (x, process.extract(x, flavor_descriptors, limit=1)))
    close_matches = string_matches.apply(lambda x: x[1][0][1] >= 90)
    pubchem_flavor = pubchem_flavor[close_matches]
    pubchem_flavor['_flavor'] = string_matches[close_matches].apply(lambda x: x[1][0][0])

    # Combine pubchem and FlavorDB flavor data
    flavor_metadata = pd.concat([flavor_metadata, pubchem_flavor], ignore_index=True)

    # # Calculate knee/elbow based on how often descriptors appear
    # descriptor_counts = flavor_metadata['_flavor'].value_counts()
    # descriptor_counts = descriptor_counts[~descriptor_counts.index.isin({"sweet", "sour", "bitter", "salty", "savory", "umami"})]
    # kneedle = kneed.KneeLocator(sorted(descriptor_counts), [x for x in range(len(descriptor_counts))])
    # valid_descriptors = set(descriptor_counts[descriptor_counts >= kneedle.knee ].keys())
    # # Keep in the 5 basic tastes, regardless of whether they fall within elbow point
    # valid_descriptors = valid_descriptors.union({"sweet", "sour", "bitter", "salty", "savory", "umami"})

    # # Eclude any flavor descriptor that do not fall within elbow range
    # flavor_metadata = flavor_metadata[flavor_metadata['_flavor'].isin(valid_descriptors)]

    # Drop duplicates
    flavor_metadata['__url'] = flavor_metadata['reference'].apply(lambda x: x['url'])
    flavor_metadata = flavor_metadata.drop_duplicates(
        subset=['source', '_pubchem_id', '_flavor', '__url'],
    )
    flavor_metadata.drop(columns=['__url'], inplace=True)

    # Set ID's for each metadata entry
    flavor_metadata.reset_index(drop=True, inplace=True)
    mf_ids = flavor_metadata.index+1
    mf_ids = "mf" + mf_ids.astype(str)
    flavor_metadata['foodatlas_id'] = mf_ids


    return flavor_metadata

# Generate new entities for each unique flavor
def generate_flavor_entities(flavor_metadata, entities):
    flavor_entities = pd.DataFrame()
    flavor_descriptors = flavor_metadata['_flavor'].unique()


    # Add new id's
    last_ent_id = entities['foodatlas_id'].apply(lambda x: int(x[1:])).max()
    flavor_entities['foodatlas_id'] = [f"e{x}" for x in range(last_ent_id+1, last_ent_id+len(flavor_descriptors)+1)]

    flavor_entities['common_name'] = flavor_descriptors
    flavor_entities['entity_type'] = "flavor"
    flavor_entities['synonyms'] = "[]"
    flavor_entities['external_ids'] = "{}"
    return flavor_entities

# Generate new triplets for each chemical -> flavor relationship
def generate_flavor_triplets(flavor_metadata, entities, triplets):
    chemical_entities = entities[entities['entity_type'] == 'chemical']
    flavor_entities = entities[entities['entity_type'] == 'flavor']

    # Head = chemical
    flavor_triplets = pd.merge(
        chemical_entities[['foodatlas_id', 'pc_id']],
        flavor_metadata[['_pubchem_id', '_flavor', 'foodatlas_id']],
        how='right',
        left_on='pc_id',
        right_on='_pubchem_id',
    )
    flavor_triplets = flavor_triplets.rename(
        columns={'foodatlas_id_x' : 'head_id', 'foodatlas_id_y' : 'metadata_ids'}
    )
    # Tail = flavor
    flavor_triplets = pd.merge(
        flavor_entities[['common_name', 'foodatlas_id']],
        flavor_triplets,
        how='right',
        left_on='common_name',
        right_on='_flavor',
    )
    flavor_triplets = flavor_triplets.rename(
        columns={'foodatlas_id' : 'tail_id', 'foodatlas_id_y' : 'tail_id'}
    )
    flavor_triplets.drop(
        ['common_name', 'pc_id', '_flavor', '_pubchem_id'],
        axis=1,
        inplace=True,
    )
    flavor_triplets['relationship_id'] = 'r5'

    # Add new id's
    next_trip_id = triplets['foodatlas_id'].apply(lambda x: int(x[1:])).max()+1
    flavor_triplets['foodatlas_id'] = [f"t{x}" for x in range(next_trip_id, next_trip_id+len(flavor_triplets))]
    flavor_triplets['metadata_ids'] = flavor_triplets['metadata_ids'].apply(lambda x: [x])
    return flavor_triplets


def generate_and_merge_flavor_data(entities_without_flavor_path, 
                                   triplets_without_flavor_path, 
                                   flavordb_data_path, flavor_meta_out,
                                   entities_flavor_out, triplets_flavor_out):
    entities = pd.read_csv(entities_without_flavor_path, delimiter="\t")
    triplets = pd.read_csv(triplets_without_flavor_path, delimiter="\t")

    # Extract pubchem id's from entity data and temporarily apply as a new column.
    entities['external_ids'] = entities['external_ids'].apply(ast.literal_eval)
    entities['pc_id'] = entities['external_ids'].apply(
        lambda x: x['pubchem_compound'][0] if 'pubchem_compound' in x else None
    ).astype(pd.Int32Dtype())

    # Generate metadata, entities, and triplets
    flavor_metadata = generate_flavor_metadata(flavordb_data_path, entities)

    flavor_entities = generate_flavor_entities(flavor_metadata, entities)
    entities = pd.concat([entities, flavor_entities], ignore_index=True)

    flavor_triplets = generate_flavor_triplets(flavor_metadata, entities, triplets)
    triplets = pd.concat([triplets, flavor_triplets], ignore_index=True)

    # Drop the pc_id column generated previously
    entities = entities.drop(columns='pc_id')

    # Clean up before exporting.
    flavor_metadata['entity_linking_method'] = 'id_matching'
    flavor_metadata['_chemical'] = flavor_metadata['_pubchem_id'].apply(
        lambda x: f"PUBCHEM_COMPOUND:{x}"
    )
    flavor_metadata = flavor_metadata.rename(
        columns={
            '_chemical': '_chemical_name',
            '_flavor': '_flavor_name',
        }
    )
    flavor_metadata = flavor_metadata[[
        'foodatlas_id',
        'source',
        'reference',
        'entity_linking_method',
        '_chemical_name',
        '_flavor_name',
    ]]

    # Export everything as tsv
    flavor_metadata.to_csv(flavor_meta_out, sep='\t', index=False)
    entities.to_csv(entities_flavor_out, sep='\t', index=False)
    triplets.to_csv(triplets_flavor_out, sep='\t', index=False)

# Paths to input entity and triplet data without flavor.
# I am using the entities/triplets with disease data from Arielle
entities_without_flavor_path = "outputs/kg/entities.tsv"
triplets_without_flavor_path = "outputs/kg/triplets.tsv"

# Output paths to tsv files that will be generated
flavor_meta_out = "outputs/kg/metadata_flavor.tsv"
entities_flavor_out = "outputs/kg/entities.tsv"
triplets_flavor_out = "outputs/kg/triplets.tsv"

# Paths to data used to generate flavor data
flavordb_data_path = "data/FlavorDB/flavordb_scrape.pt"

generate_and_merge_flavor_data(
    entities_without_flavor_path,
    triplets_without_flavor_path,
    flavordb_data_path,
    flavor_meta_out,
    entities_flavor_out,
    triplets_flavor_out,
)
