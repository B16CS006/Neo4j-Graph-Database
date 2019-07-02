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

load_small_database(server_url, username, password, database_dir)


# load_nodes(filename)




# global index
# index = 0

# def multiprocessing_fun(i,row):
#     print(i,'################e########################################', len(row))
#     global a
#     statement = a.csv_load_node(header, row)
#     a._write_transaction_(statement)

# global a
# a = DatabaseHandler('bolt://localhost:7687','neo4j', 'a392912030502')
# header, tail = a.return_head_tail('node')
# # a.close()
# number_of_lines = 48477
# # a = DatabaseHandler('bolt://localhost:7687','neo4j', 'a392912030502')

# pool = multiprocessing.Pool(multiprocessing.cpu_count()-2)
# results = [pool.apply(multiprocessing_fun, args=(i,row,)) for i, row in enumerate(tail)]
# pool.close()
# # a.close()









# global index
# index = 0

# def multiprocessing_fun(i,row):
#     a = DatabaseHandler('bolt://localhost:7687','neo4j', 'a392912030502')
#     print(i,'########################################################', len(row))
#     statement = a.csv_load_node(header, row)
#     a._write_transaction_(statement)
#     a.close()

# a = DatabaseHandler('bolt://localhost:7687','neo4j', 'a392912030502')
# header, tail = a.return_head_tail('node')
# a.close()
# number_of_lines = 48477

# pool = multiprocessing.Pool(multiprocessing.cpu_count()-2)
# results = [pool.apply(multiprocessing_fun, args=(i,row,)) for i, row in enumerate(tail)]
# pool.close()







# global index
# index = 0

# def multiprocessing_fun1(i,row,a):
#     print(i,'########################################################', len(row))
#     statement = a.csv_load_node(header, row)
#     a._write_transaction_(statement)

# def multiprocessing_fun(i, rows):
    

# a = DatabaseHandler('bolt://localhost:7687','neo4j', 'a392912030502')
# header, tail = a.return_head_tail('node')
# a.close()
# number_of_lines = 48477

# a = DatabaseHandler('bolt://localhost:7687','neo4j', 'a392912030502')
# pool = multiprocessing.Pool(multiprocessing.cpu_count()-2)
# results = [pool.apply(multiprocessing_fun, args=(i,row,)) for i, row in enumerate(tail)]
# pool.close()

# a.close()




# global index
# index = 0

# def multiprocessing_fun(i,rows):
#     a = DatabaseHandler('bolt://localhost:7687','neo4j', 'a392912030502')
#     print(i,'########################################################')
#     for row in rows:
#         global index
#         print(i,index)
#         index = index + 1
#         statement = a.csv_load_node(header, row)
#         a._write_transaction_(statement)
#         break
#     a.close()

# a = DatabaseHandler('bolt://localhost:7687','neo4j', 'a392912030502')
# header, tail = a.return_head_tail('node')
# a.close()

# number_of_lines = 48477
# processes = []
# for i in range(8):
#     print('/////////////////////////////////////////////////////')
#     rows = tail[i * number_of_lines : (i+1) * 100]
#     p = multiprocessing.Process(target=multiprocessing_fun, args=(i,rows,))
#     processes.append(p)
#     p.start()

# for process in processes:
#     process.join()
