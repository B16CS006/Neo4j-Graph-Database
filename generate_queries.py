from load_database import DatabaseHandler
import sys

if(len(sys.argv) < 2):
    database_dir = '../database/ngov2'
    file_to_save_queries = '../database/temporary_generate_queries.csv'
    start_with = 0
    max_count = 20000

    # sys.exit('Invalid Argument: Requred 2 argument but ' +  str(len(sys.argv)) + ' are provided')
elif(len(sys.argv) == 2):
    database_dir = sys.argv[1]
    file_to_save_queries = '../database/temporary_generate_queries.csv'
    start_with = 0
    max_count = 20000
elif(len(sys.argv) == 3):
    database_dir = sys.argv[1]
    file_to_save_queries = sys.argv[2]
    start_with = 0
    max_count = 20000
elif(len(sys.argv) == 4):
    database_dir = sys.argv[1]
    file_to_save_queries = sys.argv[2]
    start_with = int(sys.argv[3])
    max_count = 20000
elif(len(sys.argv) == 5):
    database_dir = sys.argv[1]
    file_to_save_queries = sys.argv[2]
    start_with = int(sys.argv[3])
    max_count = int(sys.argv[4])
else:
    database_dir = sys.argv[1]
    file_to_save_queries = sys.argv[2]
    start_with = int(sys.argv[3])
    max_count = int(sys.argv[4])

if(database_dir[-1] != '/'):
    database_dir = database_dir + '/'

username = "neo4j"
password = "a392912030502"
server_url = 'bolt://localhost:7687'

a = DatabaseHandler(server_url, username, password, dir = database_dir)
statements = a.csv_load_fields(start_with, max_count, operation_to_perform=1)

import csv

with open(file_to_save_queries, 'w') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerows(statements)

csvfile.close()