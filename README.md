# Apache_logs

This project processes the Apache logs from the DCP UI production server to Study of the added value brought to corporate customers by digital commerce platforms for travel retail 

[Dagster](https://dagster.readthedocs.io/) is utilised to:

* Extract the log data from csv files, process it, transform it and upload to a Postgres database
* Generate data plots

## Installation
Please see https://packaging.python.org/tutorials/installing-packages/ for general information on installation methods.

The following packages are required:
* [dagster](https://github.com/dagster-io/dagster)
* [dagster-pandas](https://pypi.org/project/dagster-pandas/)
* [pandas](https://pandas.pydata.org/)
* [db_toolkit](https://github.com/ib-da-ncirl/db_toolkit)
* [dagster_toolkit](https://github.com/ib-da-ncirl/db_toolkit)

Install dependencies via

    pip install -r requirements.txt

## Setup

TO DO

## Execution

From the project root directory, in a terminal window: 

To run the ETL pipeline, run:
    python apache_etl.py
    
To run the analysis pipeline, run   
    python apache_analysis.py
