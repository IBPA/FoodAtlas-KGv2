from .utils.data import load_ctd_data
from ... import KnowledgeGraph


if __name__ == '__main__':
    kg = KnowledgeGraph()
    ctd_chemdis = load_ctd_data(data_dir="data/CTD", type='chemdis')

    # Filter out those without correlations.
    ctd_chemdis = ctd_chemdis[ctd_chemdis["DirectEvidence"].notnull()]

    # Filter out those not in FA.
    mesh_fa = set()
    for _, row in kg.entities._entities.iterrows():
        if 'mesh' in row['external_ids']:
            mesh_fa.update(row['external_ids']['mesh'])
    ctd_chemdis = ctd_chemdis[ctd_chemdis['ChemicalID'].isin(mesh_fa)]

    # Dump the cleaned CTD dataset.
    ctd_chemdis.to_csv(
        "outputs/data_processing/ctd_chemdis_cleaned.tsv",
        sep='\t',
        index=False,
    )

    # Map PubMed IDs to PMCID
    with open("outputs/data_processing/CTD_pubmed_ids.txt", 'w') as f:
        pubmed_ids = set()
        for pubmed_id in ctd_chemdis['PubMedIDs']:
            pubmed_ids.add(pubmed_id)
        for pubmed_id in list(pubmed_ids):
            f.write(f"{pubmed_id}\n")
