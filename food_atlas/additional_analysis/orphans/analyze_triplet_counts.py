from ast import literal_eval

import pandas as pd
from kneed import KneeLocator
import seaborn as sns
import matplotlib.pyplot as plt


def plot_t2h(triplets_t1h, food_orphan):
    # T1H: Head (food) is orphaned.
    food_orphan_ = food_orphan.copy()
    food_orphan_['count_triplets'] \
        = food_orphan_.index.map(
            triplets_t1h['head_id'].value_counts()
        ).fillna(0).astype(int)
    food_orphan_['cum_count_triplets'] = food_orphan_['count_triplets'].cumsum()

    # Different threadholds.
    kneedle = KneeLocator(
        x=range(1, len(food_orphan_) + 1),
        y=food_orphan_['cum_count_triplets'],
        curve='concave',
        direction='increasing',
    )
    food_orphan_50 = food_orphan_[
        food_orphan_['cum_count_triplets'] <= triplets_t1h.shape[0] / 2
    ]
    food_orphan_90 = food_orphan_[
        food_orphan_['cum_count_triplets'] <= triplets_t1h.shape[0] * 0.9
    ]


    sns.set_theme(style='whitegrid')
    g = sns.lineplot(
        x=range(1, len(food_orphan_) + 1),
        y='cum_count_triplets',
        data=food_orphan_,
    )
    g.axvline(
        kneedle.knee,
        color='r',
        linestyle='--',
        label=f"Elbow: X = {kneedle.knee}, Y = {kneedle.knee_y}",
    )
    g.axvline(
        len(food_orphan_50),
        color='orange',
        linestyle='--',
        label=(
            f"50% triplet coverage: X = {len(food_orphan_50)}, "
            f"Y = {food_orphan_50.iloc[-1]['cum_count_triplets']}"
        ),
    )
    g.axvline(
        len(food_orphan_90),
        color='g',
        linestyle='--',
        label=(
            f"90% triplet coverage: X = {len(food_orphan_90)}, "
            f"Y = {food_orphan_90.iloc[-1]['cum_count_triplets']}"
        ),
    )
    g.set(
        xlabel='Number of the most frequent unique orphaned food entities',
        ylabel='Cumulative number of unique triplets',
    )
    g.legend()
    plt.tight_layout()
    plt.savefig("check_t1h.png")
    plt.close()


def plot_t2t(triplets_t1t, chemical_orphan):
    # T1T: Tail (chemical) is orphaned.
    chemical_orphan_ = chemical_orphan.copy()
    chemical_orphan_['count_triplets'] \
        = chemical_orphan_.index.map(
            triplets_t1t['tail_id'].value_counts()
        ).fillna(0).astype(int)
    chemical_orphan_['cum_count_triplets'] = chemical_orphan_['count_triplets'].cumsum()
    kneedle = KneeLocator(
        x=range(1, len(chemical_orphan_) + 1),
        y=chemical_orphan_['cum_count_triplets'],
        curve='concave',
        direction='increasing',
    )
    chemical_orphan_50 = chemical_orphan_[
        chemical_orphan_['cum_count_triplets'] <= triplets_t1t.shape[0] / 2
    ]
    chemical_orphan_90 = chemical_orphan_[
        chemical_orphan_['cum_count_triplets'] <= triplets_t1t.shape[0] * 0.9
    ]

    sns.set_theme(style='whitegrid')
    g = sns.lineplot(
        x=range(1, len(chemical_orphan_) + 1),
        y='cum_count_triplets',
        data=chemical_orphan_,
    )
    g.axvline(
        kneedle.knee,
        color='r',
        linestyle='--',
        label=f"Elbow: X = {kneedle.knee}, Y = {kneedle.knee_y}",
    )
    g.axvline(
        len(chemical_orphan_50),
        color='orange',
        linestyle='--',
        label=(
            f"50% triplet coverage: X = {len(chemical_orphan_50)}, "
            f"Y = {chemical_orphan_50.iloc[-1]['cum_count_triplets']}"
        ),
    )
    g.axvline(
        len(chemical_orphan_90),
        color='g',
        linestyle='--',
        label=(
            f"90% triplet coverage: X = {len(chemical_orphan_90)}, "
            f"Y = {chemical_orphan_90.iloc[-1]['cum_count_triplets']}"
        ),
    )
    g.set(
        xlabel='Number of the most frequent unique orphaned chemical entities',
        ylabel='Cumulative number of unique triplets',
    )
    g.legend()
    plt.tight_layout()
    plt.savefig("check_t1t.png")
    plt.close()


if __name__ == '__main__':
    triplets = pd.read_csv(
        "outputs/kg/triplets.tsv",
        sep='\t',
    )
    print(triplets)

    entities = pd.read_csv(
        "entities_with_count.tsv",
        sep='\t',
        converters={
            'synonyms': literal_eval,
            'external_ids': literal_eval,
        },
    ).set_index('foodatlas_id').sort_values('count_gpt', ascending=False)

    food_orphan = entities[
        (entities['entity_type'] == 'food') & (entities['external_ids'] == {})
    ]
    food_nonorphan = entities[
        (entities['entity_type'] == 'food') & (entities['external_ids'] != {})
    ]
    chemical_orphan = entities[
        (entities['entity_type'] == 'chemical') & (entities['external_ids'] == {})
    ]
    chemical_nonorphan = entities[
        (entities['entity_type'] == 'chemical') & (entities['external_ids'] != {})
    ]

    triplets_t0 = triplets[
        (triplets['head_id'].isin(food_nonorphan.index))
        & (triplets['tail_id'].isin(chemical_nonorphan.index))
    ]
    triplets_t2 = triplets[
        (triplets['head_id'].isin(food_orphan.index))
        & (triplets['tail_id'].isin(chemical_orphan.index))
    ]
    triplets_t1h = triplets[
        (triplets['head_id'].isin(food_orphan.index))
        & (triplets['tail_id'].isin(chemical_nonorphan.index))
    ]
    triplets_t1t = triplets[
        (triplets['head_id'].isin(food_nonorphan.index))
        & (triplets['tail_id'].isin(chemical_orphan.index))
    ]
    print(f"# T0 - No orphans           : {len(triplets_t0)}")
    print(f"# T1H - Orphaned food       : {len(triplets_t1h)}")
    print(f"# T1T - Orphaned chemical   : {len(triplets_t1t)}")
    print(f"# T2 - Orphaned both        : {len(triplets_t2)}")

    plot_t2h(triplets_t1h, food_orphan)
    plot_t2t(triplets_t1t, chemical_orphan)