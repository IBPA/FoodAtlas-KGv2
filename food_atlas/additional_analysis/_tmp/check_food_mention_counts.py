import pandas as pd


if __name__ == '__main__':
    # Load mentions.
    metadata = pd.read_csv("outputs/kg/metadata_contains.tsv", sep='\t')
    mentions = metadata['_food_name'].tolist()
    mentions = [x for x in mentions if not x.startswith('_FDC_IDS')]

    cnt = {}
    for mention in mentions:
        cnt[mention] = cnt.get(mention, 0) + 1

    mentions = metadata['_food_name'].unique().tolist()
    mentions = [x for x in mentions if not x.startswith('_FDC_IDS')]
    mentions = pd.DataFrame(mentions, columns=['NAME'])
    mentions['count'] = mentions['NAME'].apply(lambda x: cnt[x])
    mentions = mentions.set_index('NAME')
    mentions = mentions.sort_values('count', ascending=False)

    print(mentions)