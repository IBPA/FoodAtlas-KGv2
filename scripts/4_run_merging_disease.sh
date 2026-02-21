#!/bin/bash

python -m food_atlas.kg.merge_dbs.ctd.run_processing_ctd
python -m food_atlas.kg.merge_dbs.ctd.make_pmid_to_pmcid fzli@ucdavis.edu
python -m food_atlas.kg.merge_dbs.ctd.merge_ctd
