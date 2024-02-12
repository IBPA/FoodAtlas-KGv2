import pandas as pd

data = pd.read_parquet("data/FooDB/Content.parquet", engine='pyarrow')
print(data['citation_type'].value_counts(dropna=False))

# Exclude `citatin_type` of 'PREDICTED'. This means PATHBANK and HMDB are ignored.
data = data[data['citation_type'] != 'PREDICTED']

# Exclude `citatin_type` of 'DATABASE' since we will directly add them. This means large
# databases such as FDC and Frida.
data = data[data['citation_type'] != 'DATABASE']

# # 'MANUAL' is a little bit ambiguous, so we should ask FooDB about it.
data = data[data['citation_type'] == 'EXPERIMENTAL']
print(data['citation'].value_counts())
# data_manual = data[data['citation'] == 'MANUAL']
# print(data_manual['citation_type'].value_counts(dropna=False))

# data = data[data['citation_type'] == 'EXPERIMENTAL']
# print(data['food_id'].nunique())
# print(data['source_id'].nunique())
# chemicals = pd.read_json("data/FooDB/Compound.json", lines=True).set_index('id')

# def get_chemical(row):
#     if row['source_id'] in chemicals.index:
#         return chemicals.loc[row['source_id']]
#     else:
#         return

# data = pd.concat([data, data.apply(get_chemical, axis=1)], axis=1)
# print(data['source_id'].value_counts())
# data.to_csv("check_foodb_experimental.csv", index=False)
