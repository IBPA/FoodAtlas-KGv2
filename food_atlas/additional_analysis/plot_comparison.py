import pandas as pd


# # Get food names.
# data = pd.read_parquet("data/FooDB/Content.parquet", engine='pyarrow')
# print(data['citation_type'].value_counts(dropna=False))

# # Data without predicted values.
# data = data[data['citation_type'] != 'PREDICTED']
# print(data)

# foods = pd.read_json("data/FooDB/Food.json", lines=True).astype({
#     'ncbi_taxonomy_id': 'Int64'
# }).query(f"id in {data['food_id'].unique().tolist()}")

# chemicals = pd.read_json("data/FooDB/Compound.json", lines=True).query(
#     f"id in {data['source_id'].unique().tolist()}"
# )
# # chemicals['moldb_inchikey'].dropna().to_csv(
# #     "foodb_inchikeys.txt", sep='\t', header=False, index=False
# # )

# inchikey_to_cid = pd.read_csv(
#     "data/FooDB/inchikey_to_cid.txt", sep='\t', header=None, names=['inchikey', 'cid']
# )
# inchikey_to_cid = inchikey_to_cid.dropna(subset=['inchikey'])

# def get_cid(group):
#     cids = group['cid'].astype('Int64').dropna().unique().tolist()
#     inchikey_to_cid_map[group.name] = cids

# inchikey_to_cid_map = {}
# inchikey_to_cid.groupby('inchikey').apply(get_cid)

# chemicals['cids'] = chemicals['moldb_inchikey'].apply(
#     lambda inchikey: inchikey_to_cid_map.get(inchikey, [])
# )
# chemicals['num_cids'] = chemicals['cids'].apply(len)
# print(chemicals['num_cids'].value_counts())


# FDC in FooDB.
content = pd.read_parquet("data/FooDB/Content.parquet", engine='pyarrow')
content = content.query("citation_type == 'DATABASE' and citation == 'USDA'")
foods = pd.read_json("data/FooDB/Food.json", lines=True).astype({
    'ncbi_taxonomy_id': 'Int64'
}).query(f"id in {content['food_id'].unique().tolist()}")
chemicals = pd.read_json("data/FooDB/Compound.json", lines=True).query(
    "id in "
    f"{content[content['source_type'] == 'Compound']['source_id'].unique().tolist()}"
)

content_ = content.drop_duplicates(subset=['food_id', 'source_id'])

# print(content_.to_csv("content_usda.csv"))
print(foods)
print(chemicals)
# print(content.query("source_id == 4").to_csv('source_4.csv'))