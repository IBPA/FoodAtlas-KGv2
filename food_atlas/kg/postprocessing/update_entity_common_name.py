from ast import literal_eval

import pandas as pd
from pandarallel import pandarallel
from tqdm import tqdm

from ..utils import constants

tqdm.pandas()


def is_skipped(mention):
    for prefix in constants.ID_PREFIX_MAPPER.values():
        if mention.startswith(f"_{prefix}"):
            return True

    return False


# def create_synonym_cnt_dict(row, metadata):
#     synonyms = ast.literal_eval(row["synonyms"])
#     sys_cnt_dict = {}
#     for sys in synonyms:
#         if is_skipped(sys):
#             continue

#         if row["entity_type"] == 'food':
#             selected = metadata[metadata["_food_name"] == sys]
#         else:
#             selected = metadata[metadata["_chemical_name"] == sys]
#         cnt = len(selected)
#         sys_cnt_dict[sys] = cnt

#     return sys_cnt_dict


# def main():
#     pandarallel.initialize(progress_bar=True)
#     entities = pd.read_csv("outputs/kg/entities.tsv", sep="\t")
#     metadata_file = pd.read_csv("outputs/kg/metadata_contains.tsv",sep = "\t")
#     entities["synonym_counts"] = entities.parallel_apply(create_synonym_cnt_dict,
#                                                        metadata=metadata_file, axis=1)


    # def count_synonyms(row):
    #     head_id = row['head_id']
    #     tail_id = row['tail_id']
    #     metadata_ids = row['metadata_ids']

    #     metadata_ = metadata.loc[metadata_ids]
    #     head_mentions = metadata_['_food_name'].tolist()
    #     tail_mentions = metadata_['_chemical_name'].tolist()

    #     for mention in head_mentions:
    #         if not is_skipped(mention):
    #             entities.at[head_id, 'synonym_counts'][mention] += 1

    #     for mention in tail_mentions:
    #         if not is_skipped(mention):
    #             entities.at[tail_id, 'synonym_counts'][mention] += 1

    # triplets.progress_apply(count_synonyms, axis=1)

    def update_common_name(row):
        row['common_name'] = max(
            row['synonym_counts'],
            key=row['synonym_counts'].get
        )

        return row

    entities = entities.apply(update_common_name, axis=1)
    print(entities)
    entities = entities.drop(columns=['synonym_counts'])
    # entities.to_csv("outputs/kg/entities.tsv", sep='\t')


# if __name__ == "__main__":
#     main()

if __name__ == '__main__':
    entities = pd.read_csv(
        "outputs/kg/entities.tsv",
        sep='\t',
        converters={'synonyms': literal_eval}
    ).set_index('foodatlas_id')
    triplets = pd.read_csv(
        "outputs/kg/triplets.tsv",
        sep='\t',
        converters={'metadata_ids': literal_eval},
    ).set_index('foodatlas_id')
    metadata = pd.read_csv(
        "outputs/kg/metadata_contains.tsv",
        sep='\t',
    ).set_index('foodatlas_id')

    entities['synonym_counts'] = entities['synonyms'].apply(
        lambda x: dict.fromkeys(x, 0),
    )

    def is_skipped(mention):
        for prefix in constants.ID_PREFIX_MAPPER.values():
            if mention.startswith(f"_{prefix}"):
                return True

        return False

    def count_synonyms(row):
        head_id = row['head_id']
        tail_id = row['tail_id']
        metadata_ids = row['metadata_ids']

        metadata_ = metadata.loc[metadata_ids]
        head_mentions = metadata_['_food_name'].tolist()
        tail_mentions = metadata_['_chemical_name'].tolist()

        for mention in head_mentions:
            if not is_skipped(mention):
                entities.at[head_id, 'synonym_counts'][mention] += 1

        for mention in tail_mentions:
            if not is_skipped(mention):
                entities.at[tail_id, 'synonym_counts'][mention] += 1

    triplets = triplets.query("relationship_id == 'r1'")
    triplets.progress_apply(count_synonyms, axis=1)

    def update_common_name(row):
        if not row['synonym_counts']:
            return row

        row['common_name'] = max(
            row['synonym_counts'],
            key=row['synonym_counts'].get
        )
        return row

    entities = entities.apply(update_common_name, axis=1)
    entities = entities.drop(columns=['synonym_counts'])
    entities.to_csv("outputs/kg/entities.tsv", sep='\t')
