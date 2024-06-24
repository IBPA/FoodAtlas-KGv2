from ast import literal_eval

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt


def plot_entities():
    entities = pd.read_csv(
        "outputs/kg/20240410_chebi/entities.tsv",
        sep='\t',
        converters={
            'external_ids': literal_eval,
        }
    )
    entities['is_orphan'] = entities['external_ids'].apply(
        lambda x: True if len(x) == 0 else False
    )

    # Plot the number of entities per type.
    sns.set_theme(style="whitegrid")
    sns.barplot(
        data=entities[['entity_type', 'is_orphan']].value_counts().reset_index(),
        x='entity_type',
        y='count',
        hue='is_orphan',
    )
    plt.savefig("entities.png")
    plt.close()


def plot_triplets():
    entities = pd.read_csv(
        "outputs/kg/20240410_chebi/entities.tsv",
        sep='\t',
        converters={
            'external_ids': literal_eval,
        }
    ).set_index('foodatlas_id')
    entities['is_orphan'] = entities['external_ids'].apply(
        lambda x: True if len(x) == 0 else False
    )
    triplets = pd.read_csv(
        "outputs/kg/20240410_chebi/triplets.tsv",
        sep='\t',
    )

    def get_triplet_tier(row):
        head = entities.loc[row['head_id']]
        tail = entities.loc[row['tail_id']]

        if not head['is_orphan'] and not tail['is_orphan']:
            return 'Both non-orphans'
        elif head['is_orphan'] and not tail['is_orphan']:
            return 'Only head orphan'
        elif not head['is_orphan'] and tail['is_orphan']:
            return 'Only tail orphan'
        else:
            return 'Both orphans'

    triplets['tier'] = triplets.apply(get_triplet_tier, axis=1)

    sns.set_theme(style="whitegrid")
    sns.barplot(
        data=triplets['tier'].value_counts().reset_index(),
        x='tier',
        y='count',
        order=[
            'Both non-orphans', 'Only head orphan', 'Only tail orphan', 'Both orphans'
        ],
    )
    plt.savefig("triplets.png")
    plt.close()


def plot_metadata():
    metadata = pd.read_csv(
        "outputs/kg/20240410_chebi/metadata_contains.tsv",
        sep='\t',
    )
    metadata['with_standardized_concentration'] = metadata['conc_unit'].apply(
        lambda x: True if pd.notna(x) else False
    )

    # Plot the number of entities per type.
    sns.set_theme(style="whitegrid")
    sns.barplot(
        data=metadata[['source', 'with_standardized_concentration']].value_counts().reset_index(),
        x='source',
        y='count',
        hue='with_standardized_concentration',
    )
    plt.savefig("metadata.png")
    plt.close()

if __name__ == '__main__':
    # plot_entities()
    # plot_triplets()
    plot_metadata()
