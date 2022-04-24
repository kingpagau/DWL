import json
import psycopg2
import time
import requests

def lambda_handler(event, context):
    
    conn = psycopg2.connect("host=aoe21.cob0rzy4a66o.us-east-1.rds.amazonaws.com dbname=aoe21 user=tico password=1234567890")
        
    cur = conn.cursor()
    conn.set_session(autocommit = True)
    
    unix_time_stamp = int(time.time())
    while (unix_time_stamp % 600) != 0:
        unix_time_stamp = unix_time_stamp + 1
        
    request = requests.get("https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/?format=json&appid=813780")
    request = request.json()
    
    player_count = request["response"]["player_count"]
    
    cur.execute("INSERT INTO player_count(unix_time_stamp, player_count) VALUES (%s , %s);", (unix_time_stamp, player_count))

    return {
        'statusCode': 200,
        'body': json.dumps('Successfully loaded data')
    }
