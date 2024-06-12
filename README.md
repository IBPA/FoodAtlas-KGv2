# FoodAtlas
FoodAtlas is a project that aims to build a high-quality, comprehensive knowledge base on food. It uses large language models to extract relevant entities from the scientific literature and then links them to common databases. Currently, FoodAtlas contains food, chemical, and disease entities, and the coverage of information will keep on growing. The web demo can be accessed at https://www.foodatlas.ai/.

## Directories
- [`data`](./data): Repository for raw input data.
- [`food_atlas`](./food_atlas): Source code for FoodAtlas.
- [`outputs`](./outputs): Repository for intermediate and output data.
- [`scripts`](./scripts): Shell scripts.

## Getting Started
The project has been tested in the following environments:
- Ubuntu 22.04.4 LTS
- Python 3.11.5

### Clone this repository to your local machine.
```console
git clone https://github.com/IBPA/FoodAtlas-KGv2.git
cd FoodAtlas-KGv2
```

### Create an Anaconda environment.
Download and install Anaconda from [here](https://www.anaconda.com/products/distribution).
```console
conda create -n foodatlas
conda activate foodatlas
```
You can deactivate the environment with `conda deactivate`.

### Install the required packages.
```console
pip install -r requirements.txt
```

### Run the code.
- Step 1: Download the input data files. See this [README](./scripts/README.md).
- Step 2: Run the scripts. See this [README](./scripts/README.md).

## Authors
- Fangzhou Li: Graduate Student<sup>1,3,4</sup>
- Jason Youn: Graduate Student<sup>1,3,4</sup>
- Arielle Yoo: Graduate Student<sup>1,2,4</sup>
- Ilias Tagkopoulos: Principal Investigator<sup>1,3,4</sup>

1. Department of Computer Science, University of California at Davis
2. Department of Biomedical Engineering, University of California at Davis
3. Genome Center, University of California at Davis
4. USDA/NSF AI Institute for Next Generation Food Systems (AIFS)

## Contact
For any questions, you can contact Fangzhou Li (fzli@ucdavis.edu) or Prof. Ilias Tagkopoulos (itagkopoulos@ucdavis.edu).

## Citation
```bibtex
@article{youn2024foodatlas,
  title={FoodAtlas: Automated Knowledge Extraction of Food and Chemicals from Literature},
  author={Youn, Jason and Li, Fangzhou and Simmons, Gabriel and Kim, Shanghyeon and Tagkopoulos, Ilias},
  journal={bioRxiv},
  pages={2024--05},
  year={2024},
  publisher={Cold Spring Harbor Laboratory}
}
```

## License
This project is licensed under the Apache-2.0 License. Please see the [LICENSE](./LICENSE) file for details.

## Acknowledgements
Special thanks to Lukas Masopust, Ammar Ziadeh, and Kristin Singhasemanon for creating the web demo. We also thank the members of the Tagkopoulos Lab for their valuable feedback.

## Funding
This work was supported by the USDA-NIFA AI Institute for Next Generation Food Systems (AIFS), USDA-NIFA award number 2020-67021-32855.
