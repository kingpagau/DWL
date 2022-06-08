import pandas
import numpy
import psycopg2

try:
    conn = psycopg2.connect("host=aoe21.cob0rzy4a66o.us-east-1.rds.amazonaws.com dbname=aoe21 user=tico password=1234567890")
except psycopg2.Error as e:
    print("Error: Could not make connection to the Postgres database")
    print(e)

try:
    cur = conn.cursor()
except psycopg2.Error as e:
    print("Error: Could not get curser to the Database")
    print(e)

# Auto commit is very important
conn.set_session(autocommit=True)

# cur.execute("DROP TABLE match_history");
# cur.execute("DROP TABLE player_count");

cur.execute("CREATE TABLE IF NOT EXISTS match_history (match_id int, map int, server varchar(100), started int, "
            "finished int, steam_id_a bigint, "
            "steam_id_b bigint, country_a varchar(100), country_b varchar(100), rating_a int, rating_b int, "
            "rating_change_a int, "
            "rating_change_b int, civilization_a int, civilization_b int, win_a bool, win_b bool);")

cur.execute("CREATE TABLE IF NOT EXISTS player_count (unix_time_stamp bigint, player_count int);")

