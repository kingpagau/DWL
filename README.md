This document describes the functions used for the following project:

# Team: Age of Data II

### Data Warehouse and Data Lake I & II
	
Authors: 	

Lukas Bucher, 
lukas.bucher.01@stud.hslu.ch

Tihomir Danesic, 
tihomir.danesic@stud.hslu.ch

Pascal Wälti, 
pascal.waelti@stud.hslu.ch

## DWL II

### Table of Scripts
#### Jupyter Notebooks:
- API_Herokuapp_for_CLI.ipynb (CLI_pipeline)
- API_Match_History_For_Lambda.ipynb (exploratory_and_commented_code)
- API_Player_Count_For_Lambda.ipynb (exploratory_and_commented_code)
- API_Match_History.ipynb
- API_Player_Count.ipynb
- APIs.ipynb

#### AWS Lamda functions:
- aoe_match_history_lambda_function.py (lamda_functions)
- aoe_player_count_lamda_function.py (lamda_functions)

### Usage of Scripts

The Jupyter Notebooks

- API_Match_History_For_Lambda.ipynb
- API_Player_Count_For_Lambda.ipynb

are in the Repo for completeness of documentation. While these two scripts also contain test and explorational code as well as comments for explanation. The scripts contain the code to request a dynamic API and load the data into the a PostgreSQL database living in the AWS environment. 

The Jupyter Notebook

- API_Herokuapp_for_CLI.ipynb

contains the code to ingest a static API via JSON-file creation and uploads the files to an AWS S3 Bucket via AWS CLI. 

The Python scripts are used to formulate the lambda functions and to setup the database tables

- aoe_match_history_lambda_function.py
- aoe_player_count_lamda_function.py
- Connect Database - Create tables.py

Additionally the Jupyter Notebooks to analyse the APIs and execute stresstests are also provided

- API_Match_History.ipynb
- API_Player_Count.ipynb
- APIs.ipynb

are the implementation of the described Jupyter Notebooks above as a AWS Lambda function in the AWS surrounding. The Lamda Functions are both triggered every 10 minutes by AWS EventBridge (CloudWatch Events).


## DWL II

### Table of Scripts
#### Jupyter Notebooks:
-
-
-
-

#### AWS Lamda functions:
-
-


## Access to PostgreSQL database and AWS S3 Bucket

All databases and the S3 Bucket are publicly available.

However if you need the user credentials in order to upload the static API to the S3 Bucket please contact pascal.waelti@stud.hslu.com. The hardcoded credentials in the code need to updated by for every new Learner Lab - Foundational Services session.
