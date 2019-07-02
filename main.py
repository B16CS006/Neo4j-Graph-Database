from load_database import DatabaseHandler
import multiprocessing
import sys

def load_nodes(filename):
    a = DatabaseHandler('bolt://localhost:7687','neo4j', 'a392912030502', dir='../database/temp/')
    # header, tail = a.return_head_tail(filename)
    # print(header)
    # print(tail[0])
    # return
    a.csv_load_nodes(filename = filename)

if(len(sys.argv) != 2):
    sys.exit('Invalid Argument: Requred 2 argument but ' +  str(len(sys.argv)) + ' are provided')
else:
    filename = sys.argv[1]
    ext = filename.split('.')   [-1]
    if(ext != 'csv'):
        sys.exit('Invalid file type: Required \'csv\' but \'' + ext + '\' is provided')

load_nodes(filename)




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
