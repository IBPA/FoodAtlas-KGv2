wget https://ctdbase.org/reports/CTD_chemicals.csv.gz -P data/
wget https://ctdbase.org/reports/CTD_diseases.csv.gz -P data/
wget https://ctdbase.org/reports/CTD_chemicals_diseases.csv.gz -P data/

gunzip data/CTD_chemicals.csv.gz data/CTD_diseases.csv.gz data/CTD_chemicals_diseases.csv.gz