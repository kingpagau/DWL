import json
import psycopg2
import psycopg2.extras as extras

import requests
import time
import pandas as pd
import numpy as np


def lambda_handler(event, context):
    # access source RDS source database (datalake)
    try:
        conn = psycopg2.connect(
            "host=aoe21.cob0rzy4a66o.us-east-1.rds.amazonaws.com dbname=aoe21 user=tico password=1234567890")
    except psycopg2.Error as e:
        print("Error: Could not make connection to the Postgres database")
        print(e)

    # establish cursor for source
    try:
        cur = conn.cursor()
    except psycopg2.Error as e:
        print("Error: Could not get curser to the Database")
        print(e)
    conn.set_session(autocommit=True)

    # access target RDS target database (datawarehouse)
    try:
        conn_target = psycopg2.connect(
            "host=aoe2warehouse.cm7nj2633ign.us-east-1.rds.amazonaws.com dbname=aoe2warehouse user=pasci password=123456789")
    except psycopg2.Error as e:
        print("Error: Could not make connection to the Postgres database")
        print(e)

    # establish cursor for target
    try:
        cur_target = conn_target.cursor()
    except psycopg2.Error as e:
        print("Error: Could not get curser to the Database")
        print(e)
    conn_target.set_session(autocommit=True)

    # only select first 250'000 entries
    postgreSQL_select_Query = "SELECT * FROM match_history LIMIT 40000"
    cur.execute(postgreSQL_select_Query)

    # Select all data from table player_count
    match_history = cur.fetchall()
    match_history = pd.DataFrame(match_history)

    match_history.columns = ['match_id', 'map', 'server', 'started', 'finished', 'steam_id_a', 'steam_id_b',
                             'country_a', 'country_b', 'rating_a', 'rating_b', 'rating_change_a', 'rating_change_b',
                             'civilization_a', 'civilization_b', 'win_a', 'win_b']

    # create dublicate
    match_history_adjusted = match_history

    # rename map to map_id
    match_history.rename(columns={"map": "map_id"}, inplace=True)

    # delete matches with no winner or steam id
    # those matches are not suitable for the visualization
    match_history_adjusted = match_history_adjusted.dropna(axis=0, subset=["win_a"])
    match_history_adjusted = match_history_adjusted.dropna(axis=0, subset=["win_b"])
    match_history_adjusted = match_history_adjusted.dropna(axis=0, subset=["steam_id_a"])
    match_history_adjusted = match_history_adjusted.dropna(axis=0, subset=["steam_id_b"])

    # drop rating_change_a and rating_change_b
    # information not suitable for the visualization
    match_history_adjusted = match_history_adjusted.drop(["rating_change_a", "rating_change_b"], axis=1)

    # categorize matchups into winner and loser information instead of a and b
    match_history_adjusted["steam_id_win"] = match_history_adjusted.apply(
        lambda x: x["steam_id_a"] if x["win_a"] == True else x["steam_id_b"], axis=1)
    match_history_adjusted["steam_id_lose"] = match_history_adjusted.apply(
        lambda x: x["steam_id_b"] if x["win_a"] == True else x["steam_id_a"], axis=1)
    match_history_adjusted["country_win"] = match_history_adjusted.apply(
        lambda x: x["country_a"] if x["win_a"] == True else x["country_b"], axis=1)
    match_history_adjusted["country_lose"] = match_history_adjusted.apply(
        lambda x: x["country_b"] if x["win_a"] == True else x["country_a"], axis=1)
    match_history_adjusted["rating_win"] = match_history_adjusted.apply(
        lambda x: x["rating_a"] if x["win_a"] == True else x["rating_b"], axis=1)
    match_history_adjusted["rating_lose"] = match_history_adjusted.apply(
        lambda x: x["rating_b"] if x["win_a"] == True else x["rating_a"], axis=1)
    match_history_adjusted["civilization_win"] = match_history_adjusted.apply(
        lambda x: x["civilization_a"] if x["win_a"] == True else x["civilization_b"], axis=1)
    match_history_adjusted["civilization_lose"] = match_history_adjusted.apply(
        lambda x: x["civilization_b"] if x["win_a"] == True else x["civilization_a"], axis=1)

    request = requests.get("https://aoe2.net/api/strings?game=aoe2de&language=en")
    request = request.json()

    # read in civilization keys
    civilizations = request["civ"]
    civilizations = pd.DataFrame.from_dict(civilizations)

    # replace civilization key with civilization name for civilization_win
    match_history_adjusted = pd.merge(match_history_adjusted, civilizations, how="left", left_on="civilization_win",
                                      right_on="id")
    match_history_adjusted = match_history_adjusted.drop(["civilization_win", "id"], axis=1)
    match_history_adjusted = match_history_adjusted.rename(columns={"string": "civilization_win"})

    # replace civilization key with civilization name for civilization_lose
    match_history_adjusted = pd.merge(match_history_adjusted, civilizations, how="left", left_on="civilization_lose",
                                      right_on="id")
    match_history_adjusted = match_history_adjusted.drop(["civilization_lose", "id"], axis=1)
    match_history_adjusted = match_history_adjusted.rename(columns={"string": "civilization_lose"})

    # read in map keys
    maps = request["map_type"]
    maps = pd.DataFrame.from_dict(maps)

    # replace map key with map name
    match_history_adjusted = pd.merge(match_history_adjusted, maps, how="left", left_on="map_id", right_on="id")
    match_history_adjusted = match_history_adjusted.drop(["map_id", "id"], axis=1)
    match_history_adjusted = match_history_adjusted.rename(columns={"string": "map"})

    # decode country keys via additional api
    request = requests.get("https://restcountries.com/v3.1/all")
    request = request.json()

    # read in country keys
    countries_common_name = [d["name"]["common"] for d in request]
    countries_cca2 = [d["cca2"] for d in request]
    countries = {"common_name": countries_common_name, "cca2": countries_cca2}
    countries = pd.DataFrame(countries)

    # replace country key with country name for country_win
    match_history_adjusted = pd.merge(match_history_adjusted, countries, how="left", left_on="country_win",
                                      right_on="cca2")
    match_history_adjusted = match_history_adjusted.drop(["country_win", "cca2"], axis=1)
    match_history_adjusted = match_history_adjusted.rename(columns={"common_name": "country_win"})

    # replace country key with country name for country_lose
    match_history_adjusted = pd.merge(match_history_adjusted, countries, how="left", left_on="country_lose",
                                      right_on="cca2")
    match_history_adjusted = match_history_adjusted.drop(["country_lose", "cca2"], axis=1)
    match_history_adjusted = match_history_adjusted.rename(columns={"common_name": "country_lose"})

    # calculate match duration in minutes
    match_history_adjusted["duration_min"] = match_history_adjusted["finished"] - match_history_adjusted["started"]
    match_history_adjusted["duration_min"] = match_history_adjusted["duration_min"] / 60
    match_history_adjusted["duration_min"] = round(match_history_adjusted["duration_min"])

    # unix_time_stamp to datetime for started
    match_history_adjusted["started_datetime"] = pd.to_datetime(match_history_adjusted["started"], unit="s")
    match_history_adjusted = match_history_adjusted.drop(["started"], axis=1)

    # unix_time_stamp to datetime for finished
    match_history_adjusted["finished_datetime"] = pd.to_datetime(match_history_adjusted["finished"], unit="s")
    match_history_adjusted = match_history_adjusted.drop(["finished"], axis=1)

    # convert datatypes
    match_history_adjusted["steam_id_win"] = match_history_adjusted["steam_id_win"].astype("Int64")
    match_history_adjusted["steam_id_lose"] = match_history_adjusted["steam_id_lose"].astype("Int64")
    # match_history_adjusted["rating_win"] = match_history_adjusted["rating_win"].fillna(0).astype("Int64")
    # match_history_adjusted["rating_lose"] = match_history_adjusted["rating_lose"].fillna(0).astype("Int64")
    match_history_adjusted["duration_min"] = match_history_adjusted["duration_min"].astype("Int64")

    cur_target.execute("DROP TABLE match_history;")

    # Create Table if it not exists
    cur_target.execute(
        "CREATE TABLE IF NOT EXISTS match_history (match_id int, server varchar(100), steam_id_a bigint, steam_id_b bigint, country_a varchar(100), country_b varchar(100), rating_a varchar(100), rating_b varchar(100), civilization_a int, civilization_b int, win_a bool, win_b bool, steam_id_win bigint ,steam_id_lose bigint, rating_win varchar(100), rating_lose varchar(100), civilization_win varchar(100), civilization_lose varchar(100), map varchar(100), country_win varchar(100), country_lose varchar(100), duration_min int, started_datetime timestamp, finished_datetime timestamp);")

    def execute_values(conn, df, table):

        tuples = [tuple(x) for x in df.to_numpy()]

        cols = ','.join(list(df.columns))
        # SQL query to execute
        query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
        cursor = conn.cursor()
        try:
            extras.execute_values(cursor, query, tuples)
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            conn.rollback()
            cursor.close()
            return 1

        cursor.close()

    execute_values(conn_target, match_history_adjusted, 'match_history')

    return {
        'statusCode': 200,
        'body': json.dumps('Successfull data load')
    }
