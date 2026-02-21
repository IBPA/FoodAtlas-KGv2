# FoodAtlas Data Workspace
This directory holds the reference ontologies, curated datasets, and model outputs required to rebuild the FoodAtlas knowledge graph. Most public resources can be fetched automatically; a few restricted datasets must be supplied manually.

## 1. Prerequisites
Ensure you have `curl` and `unzip` installed. On Ubuntu:
```console
sudo apt-get update && sudo apt-get install curl unzip
```

## 2. Download supported archives
Run the helper script from the repository root or directly inside `data/`:
```console
cd data
./download.sh
cd ..
```
The script retrieves and unpacks the following archives into this directory:
- `CDNO/` — Common Data on Nutrition Ontology snapshot.
- `ChEBI/` — Chemical Entities of Biological Interest export curated for FoodAtlas.
- `FDC/` — FoodData Central aggregates used for nutrition metadata.
- `FlavorDB/` — Flavor compound information.
- `FoodOn/` — Food ontology for hierarchical grouping.
- `HSDB/` — Hazardous Substances Data Bank export.
- `Lit2KG/` — LLM-extracted sentences and metadata from literature.
- `MeSH/` — Medical Subject Headings ontology.
- `PubChem/` — Chemical property records.
- `FlavorGraph/` — Graph representation of ingredient–flavor relationships.

After the script completes, confirm the directories exist:
```console
ls data
```

## 3. Manually managed inputs
Some proprietary or large datasets cannot be mirrored in this repository. Populate the following directories yourself if needed:
- `CTD/` — Comparative Toxicogenomics Database subset.
- `FooDB/` — Comprehensive food and chemistry database (requires account on foodb.ca).
- `PreviousFAKGs/` — Historical FoodAtlas exports used for regression checks.
- `_PIES/` — Partner-provided ingestion data (leave empty if you do not have access).
- `download.sh` comments list optional alternative download locations (e.g., USDA FDC, FooDB). Replace or augment the script if you prefer sourcing data directly from upstream providers.

## 4. Keeping data current
- Rerun `./download.sh` whenever updated archives become available. Existing folders will be overwritten if the zip contents have newer timestamps.
- To add a new dataset, place the files in this directory and update the script plus documentation so other users understand the source and intended use.

With the data in place, continue with the main setup steps in the repository `README.md` to initialise and expand the knowledge graph.
