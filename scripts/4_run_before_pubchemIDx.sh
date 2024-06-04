./scripts/download_ctd.sh
./scripts/download_tox.sh
python -u -m fadisease.read_ctd_chemdis
python -u -m fadisease.compare_factd_chem