from load_database import DatabaseHandler
import multiprocessing
import sys



def load_small_database(server_url, username, password, database_dir = './'):
    if(database_dir[-1] != '/'):
        database_dir = database_dir + '/'
    a = DatabaseHandler(server_url, username, password, database_dir)
    a.csv_load_nodes()
    a.csv_load_taxonomy_terms()
    a.csv_load_field_collection_items()
    print('Database is successfully loaded')
    return

def load_big_database(server_url, username, password, database_dir = './'):
    load_nodes(server_url, username, password, database_dir)
    load_taxonomy_terms(server_url, username, password, database_dir)
    load_field_collection_items(server_url, username, password, database_dir)
    print('Database is successfully loaded')

def load_nodes(server_url, username, password, database_dir):
    a = DatabaseHandler(server_url, username, password, dir = database_dir)
    a.csv_load_nodes()

def load_taxonomy_terms(server_url, username, password, database_dir):
    a = DatabaseHandler(server_url, username, password, dir = database_dir)
    a.csv_load_taxonomy_terms()

def load_field_collection_items(server_url, username, password, database_dir):
    a = DatabaseHandler(server_url, username, password, dir = database_dir)
    a.csv_load_field_collection_items()



if(len(sys.argv) != 2):
    sys.exit('Invalid Argument: Requred 2 argument but ' +  str(len(sys.argv)) + ' are provided')
else:
    database_dir = sys.argv[1]

username = "neo4j"
password = "a392912030502"
server_url = 'bolt://localhost:7687'

a = DatabaseHandler(server_url, username, password, dir = database_dir)

while(True):
    try:
        print('\n\n')
        print('1: Create New Database')
        print('2: Delete Whole Database')
        print('3: Count Nodes')
        choice = int(input('Enter choice : '))
        if(choice < 0):
            a.close()
            break
        elif(choice == 0):
            load_big_database(server_url, username, password, database_dir)
        elif(choice == 1):
            load_small_database(server_url, username, password, database_dir)
        elif(choice == 2):
            a.delete_whole_database()
        else:
            print(a.count_nodes())
    except Exception as e:
        print('\n', e, '\n')