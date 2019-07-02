from load_database import DatabaseHandler
import sys

if(len(sys.argv) < 2):
    database_dir = '../database/drupal'
    start_with = 0
    max_count = 20000
    # sys.exit('Invalid Argument: Requred 2 argument but ' +  str(len(sys.argv)) + ' are provided')
elif(len(sys.argv) == 2):
    database_dir = sys.argv[1]
    start_with = 0
    max_count = 20000
elif(len(sys.argv) == 3):
    database_dir = sys.argv[1]
    start_with = int(sys.argv[2])
    max_count = 20000
elif(len(sys.argv) == 4):
    database_dir = sys.argv[1]
    start_with = int(sys.argv[2])
    max_count = int(sys.argv[3])
else:
    database_dir = sys.argv[1]
    start_with = int(sys.argv[2])
    max_count = int(sys.argv[3])

if(database_dir[-1] != '/'):
    database_dir = database_dir + '/'

username = "neo4j"
password = "a392912030502"
server_url = 'bolt://localhost:7687'

a = DatabaseHandler(server_url, username, password, dir = database_dir)
a.csv_load_nodes(start_with, max_count)