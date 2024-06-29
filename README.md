# arXiv keywords project
In this project, we aim to investigate the main topics of investigation per category in arXiv papers.
To do so, we take the ArXiv dataset by the Cornell University available in Kaggle ([link](https://www.kaggle.com/datasets/Cornell-University/arxiv)). We then count the number of occurrences of each keyword (excluding stopwords) in the abstracts of the papers. Finally, we visualize the results.

# Usage
This project uses [PySpark](https://spark.apache.org/docs/latest/api/python/index.html) with Python 3+ to process the data. To run the project, you must:
0. previously configure your Spark environment
1. copy the project to your workspace and install the requirements with `pip install -r requirements.txt`
2. download and uncompress the json snapshot of the arXiv dataset, configure `project_config.py` with the path to the dataset. You can optionally configure the path to the output files. We avoided using env variables as several projects may be running in the same environment and it could lead to conflicts by naming collisions.
3. run `python3 split_dataset.py`
4. run `python3 process_datasets.py`


## Requirements

* Python 3
* PySpark
* Install the required packages with `pip install -r requirements.txt`