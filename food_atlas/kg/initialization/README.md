# Initialization for FoodAtlas Knowledge Graph

TODO: Need to update later.

The initialization of the FoodAtlas Knowledge Graph (KG) is a process that builds basic entities, lookup tables, and metadata. The initialization starts with extracting existing NCBI Taxonomy IDs and PubChem CIDs from USDA FDC and FooDB. The extracted IDs are then used to retrieve synonyms from the respective databases and included under the entities. Each synonym is then used to create entries for the lookup tables. Lastly, the metadata, such as concentration values, are imported from the databases.

## How to run the initialization.

```console
$ ./food_atlas/kg/initialization/prepare.sh
```
