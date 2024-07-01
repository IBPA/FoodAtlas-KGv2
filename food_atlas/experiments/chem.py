import pandas as pd


if __name__ == '__main__':
    # Load Mention-to-CID mapping.
    mention_to_cid = pd.read_csv(
        "data/PubChem/mention_to_cid.txt",
        sep="\t",
        header=None,
        names=['mention', 'cid'],
        dtype={'mention': str, 'cid': 'Int64'}
    )

    sids = pd.read_parquet(
        "outputs/data_processing/SID-Map-ChEBI.parquet"
    )
    print(sids.query("registry_id == 'CHEBI:23053'"))
    exit()

    # Load Mention-to-ChEBI mapping.
    mention_to_chebi = pd.read_csv(
        "data/PubChem/cid_to_chebi.txt",
        sep="\t",
        header=None,
        names=['cid', 'chebi'],
        dtype={'cid': int, 'chebi': str}
    )

    # Check overlap.
    chebi = pd.read_csv(
        "data/ChEBI/compounds.tsv", sep='\t', encoding='latin1'
    ).set_index('CHEBI_ACCESSION')

    chebi_ids_in = mention_to_chebi['chebi'].dropna().unique().tolist()
    print(len(chebi_ids_in))

    chebi_ = chebi.loc[chebi.index.isin(chebi_ids_in)]
    print(chebi_['STAR'].value_counts())

    exit()

    print(mention_to_chebi)
    orphans = mention_to_cid[mention_to_cid['cid'].isnull()]

    # # Check ambiguous mentions.
    # mentions_ = mention_to_cid.dropna(subset=['cid']).groupby('mention')['cid'].apply(list).reset_index()
    # mentions_.to_csv("chcek.tsv", sep='\t')
    # print(mentions_)

    mention_to_cid = mention_to_cid.groupby('cid')['mention'].apply(list).reset_index()
    print(mention_to_cid)

