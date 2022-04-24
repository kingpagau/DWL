This document describes the functions used for the following project:

## Team: Age of Data II

### Data Warehouse and Data Lake I & II
	
Authors: 	Lukas Bucher
13-927-884
lukas.bucher.01@stud.hslu.ch

Tihomir Danesic
16-660-334
tihomir.danesic@stud.hslu.ch

Pascal Wälti
13-917-448
pascal.waelti@stud.hslu.ch

### Table of Scripts
#### Jupyter Notebooks:
- API_Herokuapp_for_CLI.ipynb
- API_Match_History_For_Lambda.ipynb
- API_Player_Count_For_Lambda.ipynb
#### AWS Lamda functions:
- aoe_match_history_lambda_function.py
- aoe_player_count_lamda_function.py

### Usage of Scripts

The Jupyter Notebooks

- API_Match_History_For_Lambda.ipynb
- API_Player_Count_For_Lambda.ipynb

are in the Repo for completeness of documentation. While these two scripts also contain test and explorational code as well as comments for explanation. The scripts contain the code to request a dynamic API and load the data into the a PostgreSQL database living in the AWS environment. 

The Jupyter Notebook

- API_Herokuapp_for_CLI.ipynb

contains the code to ingest a static API via JSON-file creation and uploads the files to an AWS S3 Bucket via AWS CLI. 

The the scripts

- aoe_match_history_lambda_function.py
- aoe_player_count_lamda_function.py

are the implementation of the described Jupyter Notebooks above as a AWS Lambda function in the AWS surrounding. The Lamda Functions are both triggered every 10 minutes by AWS EventBridge (CloudWatch Events).

### Access to PostgreSQL database and AWS S3 Bucket

All databases and the S3 Bucket are publicly available.

However if you need the user credentials in order to upload the static API to the S3 Bucket please contact pascal.waelti@stud.hslu.com. The hardcoded credentials in the code need to updated by for every new Learner Lab - Foundational Services session.
