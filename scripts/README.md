# Instruction: Run the scripts
There are four numbered scripts indicating the order in which they should be run. This document provides instructions on how to run each of them.

## 0_run_kg_init.sh
This script initializes the knowledge graoh (KG) by creating entities. Several empty files are also created for subsequent KG construction.

All files will be generated in `outputs/kg` directory.

```console
./scripts/0_run_kg_init.sh
```

## 1_run_metadata_processing.sh
This script processes the metadata extracted from the scientific literature by calling `food_atlas.kg.run_metadata_processing` python script. It performs necessary standardization for the metadata, such as chemical name, concentration, for the subsequent KG construction.

Please change the arguments in the script.

### Arguments:
- `PATH_INPUT`: The path to the input metadata file.
- (Optional) `PATH_OUTPUT`: The path to the output directory.

### Outputs:
Two files will be generated in `outputs/kg` directory:
- `_metadata_new.tsv`: The processed metadata.
- `_tuples_not_parsed.tsv`: The metadata entries failed to be parsed due to extraction error.

```console
./scripts/1_run_metadata_processing.sh
```

## 2_run_adding_triplets_from_metadata.sh
This expands the KG from the metadata by calling `food_atlas.kg.run_kg_expansion`. At the end of expansion, a unit test is called to check the correctness of the KG.

Please change the arguments in the script.

### Arguments:
- `PATH_INPUT`: The processed metadata.
- (Optional) `PATH_INPUT_KG`: The path to the input directory.
- (Optional) `PATH_OUTPUT_DIR`: The path to the output directory.

## 3_run_postprocessing.sh
Lastly, a postprocessing step to improve the quality of KG. For example, generating food and chemical groups, prioritize display name based on the most commonly used name, etc.
