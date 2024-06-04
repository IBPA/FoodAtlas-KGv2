echo "Input email address for PMC ID Converter API:"
read email
python -u -m fadisease.compare_factd_chem
python -u -m fadisease.make_pmid_to_pmcid $email
python -u -m fadisease.create_factd_data
python -u -m fadisease.create_fatox_data