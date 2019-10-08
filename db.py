import mysql.connector
from mysql.connector import Error
import requests
import json
import time
import datetime

ts = time.time()
timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

def get_all_keys(block_ids, data):
    """ Return sorted list of JSON keys"""
    keys = []
    for block in block_ids:
        keys = keys + list(data[block].keys())
    return sorted(set(keys))

def xstr(s):
    """ Convert JSON value to string"""
    if s is None:
        return ''
    return str(s)

def insert_data(block_dict, columns, timestamp):
    """ Generate data tuple for connector.execute()"""
    insert_list = []
    for col in columns:
        insert_list.append(xstr(block_dict.get(col)))
    insert_list.append(timestamp)
    return tuple(insert_list)

# Load data from API
url = "http://analytics.skillfactory.ru:5000/api/v1.0/get_structure_course/"
answer = requests.post(url)
data = json.loads(answer.text)

# Get list of blocks ids
block_ids = data['blocks'].keys()

# Get list of all JSON keys needed to be created in database
columns = get_all_keys(block_ids, data['blocks'])

# Generate query string for table creation and data insertion
column_create_string = ""
column_insert_string = ""
for col in columns:
    column_create_string = column_create_string + col + " TEXT, "
    column_insert_string = column_insert_string + col + ", "

create_string = "CREATE TABLE course_structure (" + column_create_string + " update_time DATETIME)"
insert_string = "INSERT INTO course_structure (" + column_insert_string + " update_time) VALUES(" + "%s," * (len(columns)) + "%s)"

try:
    # Connect to DB
    cnx = mysql.connector.connect(user='XXX', password='XXX',
                              host='XXX',
                              database='XXX')
    cursor = cnx.cursor()

    # Drop table and create new
    cursor.execute("DROP TABLE IF EXISTS course_structure")
    cursor.execute(create_string)

    # Get current time
    ts = time.time()
    timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

    # Get block data and insert to DB
    for block_id in list(block_ids):
        sql_data = insert_data(data['blocks'][block_id], columns, timestamp)
        cursor.execute(insert_string, sql_data)
        cnx.commit()
    
    cnx.close()

except Error as error:
    print(error)