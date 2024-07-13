#!/bin/bash

# Merge with CTD.
python -m food_atlas.kg.merge_dbs.ctd.run_processing_ctd
python -m food_atlas.kg.merge_dbs.ctd.merge_ctd
