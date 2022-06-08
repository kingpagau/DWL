import json
import time
import requests
import psycopg2

def lambda_handler(event, context):
    
    conn = psycopg2.connect("host=aoe21.cob0rzy4a66o.us-east-1.rds.amazonaws.com dbname=aoe21 user=tico password=1234567890")
    
    cur = conn.cursor()
    conn.set_session(autocommit = True)
    
    unix_time_stamp = int(time.time())
    while (unix_time_stamp % 600) != 0:
        unix_time_stamp = unix_time_stamp + 1
    
    unix_time_stamp = unix_time_stamp - 86400
    
    subsq_unix_time_stamp = unix_time_stamp + 600
    subsq_request = requests.get("https://aoe2.net/api/matches?game=aoe2de&count=1&since=" + str(subsq_unix_time_stamp))
    subsq_request = subsq_request.json()
    
    request = requests.get("https://aoe2.net/api/matches?game=aoe2de&count=1000&since=" + str(unix_time_stamp))
    request = request.json()
    
    i = 0
    while request[i]["match_id"] != subsq_request[0]["match_id"]:
        match_id = request[i]["match_id"]
        map = request[i]["map_type"]
        leaderboard_id = request[i]["leaderboard_id"]
        if leaderboard_id != 3:
            i += 1
            continue
        server = request[i]["server"]
        started = request[i]["started"]
        finished = request[i]["finished"]
        steam_id_a = request[i]["players"][0]["steam_id"]
        steam_id_b = request[i]["players"][1]["steam_id"]
        country_a = request[i]["players"][0]["country"]
        country_b = request[i]["players"][1]["country"]
        rating_a = request[i]["players"][0]["rating"]
        rating_b = request[i]["players"][1]["rating"]
        rating_change_a = request[i]["players"][0]["rating_change"]
        rating_change_b = request[i]["players"][1]["rating_change"]
        civilization_a = request[i]["players"][0]["civ"]
        civilization_b = request[i]["players"][1]["civ"]
        win_a = request[i]["players"][0]["won"]
        win_b = request[i]["players"][1]["won"]
        
        cur.execute("INSERT INTO match_history(match_id, map, server, started, finished, steam_id_a, steam_id_b, country_a, country_b, rating_a, rating_b, rating_change_a, rating_change_b, civilization_a, civilization_b, win_a, win_b) VALUES (%s , %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);", (match_id, map, server, started, finished, steam_id_a, steam_id_b, country_a, country_b, rating_a, rating_b, rating_change_a, rating_change_b, civilization_a, civilization_b, win_a, win_b))
    
        i += 1
    
    return {
        'statusCode': 200,
        'body': json.dumps('Successfully loaded data')
    }
