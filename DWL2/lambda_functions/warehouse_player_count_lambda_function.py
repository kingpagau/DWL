import json
import time

import pandas as pd
import numpy as np

import psycopg2
import psycopg2.extras as extras


def lambda_handler(event, context):
    try:
        conn = psycopg2.connect(
            "host=aoe21.cob0rzy4a66o.us-east-1.rds.amazonaws.com dbname=aoe21 user=tico password=1234567890")
    except psycopg2.Error as e:
        print("Error: Could not make connection to the Postgres database")
        print(e)

    try:
        cur = conn.cursor()
    except psycopg2.Error as e:
        print("Error: Could not get curser to the Database")
        print(e)
    conn.set_session(autocommit=True)

    try:
        conn_target = psycopg2.connect(
            "host=aoe2warehouse.cm7nj2633ign.us-east-1.rds.amazonaws.com dbname=aoe2warehouse user=pasci password=123456789")
    except psycopg2.Error as e:
        print("Error: Could not make connection to the Postgres database")
        print(e)

    try:
        cur_target = conn_target.cursor()
    except psycopg2.Error as e:
        print("Error: Could not get curser to the Database")
        print(e)
    conn_target.set_session(autocommit=True)

    postgreSQL_select_Query = "SELECT * FROM player_count"
    cur.execute(postgreSQL_select_Query)

    # Select all data from table player_count
    player_count = cur.fetchall()
    player_count = pd.DataFrame(player_count)

    player_count.columns = ['unix_time_stamp', 'player_count']

    player_count_adjusted = player_count

    # unix_time_stamp to datetime
    player_count_adjusted["datetime"] = pd.to_datetime(player_count_adjusted["unix_time_stamp"], unit="s")
    player_count_adjusted = player_count_adjusted[["datetime", "player_count"]]

    # replace peaks with NaN (peaks smaller than 7000 counts)
    player_count_adjusted.loc[player_count_adjusted.player_count < 7000, "player_count"] = np.nan
    player_count_adjusted[player_count_adjusted.isna().any(axis=1)]

    # linear interpolation for matching the pattern
    player_count_adjusted["player_count"] = player_count_adjusted["player_count"].interpolate()
    player_count_adjusted[player_count_adjusted.isna().any(axis=1)]

    # convert player count to integer
    player_count_adjusted["player_count"] = player_count_adjusted["player_count"].astype("int")

    # Create Table if it not exists
    cur_target.execute("CREATE TABLE IF NOT EXISTS player_count (datetime timestamp, player_count int);")

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

    execute_values(conn_target, player_count_adjusted, 'player_count')

    return {
        'statusCode': 200,
        'body': json.dumps('Data successfully loaded!')
    }