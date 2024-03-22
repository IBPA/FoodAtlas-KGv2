from ast import literal_eval

import pandas as pd
from kneed import KneeLocator
import seaborn as sns
import matplotlib.pyplot as plt


if __name__ == '__main__':
    entities = pd.read_csv(
        "entities_with_count.tsv",
        sep='\t',
        converters={
            'synonyms': literal_eval,
            'external_ids': literal_eval,
        },
    )
    entities = entities.sort_values('count_gpt', ascending=False)
    entities['cum_count_gpt'] = entities['count_gpt'].cumsum()

    kneedle = KneeLocator(
        range(1, len(entities) + 1),
        entities['cum_count_gpt'],
        curve='concave',
        direction='increasing',
    )

    # Plot the cumulate count of entities.
    sns.set_theme(style='whitegrid')
    g = sns.lineplot(
        x=range(1, len(entities) + 1),
        y='cum_count_gpt',
        data=entities,
    )
    g.axvline(
        kneedle.knee,
        color='r',
        linestyle='--',
        label=f"Elbow: X = {kneedle.knee}, Y = {kneedle.knee_y}",
    )
    g.set_xlabel('Number of the most frequent entities')
    g.set_ylabel('Cumulative frequency of entities')
    g.legend()
    plt.tight_layout()
    plt.savefig("check.png")

    # entities = entities.iloc[:kneedle.knee]
    # entities.to_csv("entities_with_count_filtered.csv", index=False)

    nonorphans = entities[entities['external_ids'] != {}]
    orphans_food = entities[
        (entities['entity_type'] == 'food')
        & (entities['external_ids'] == {})
    ].iloc[:540]
    orphans_chemical = entities[
        (entities['entity_type'] == 'chemical')
        & (entities['external_ids'] == {})
    ].iloc[:259]

    entities_ = pd.concat([nonorphans, orphans_food, orphans_chemical])
    print(entities_)
    print(entities)
    print(entities_['count_gpt'].sum())