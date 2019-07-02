# Neo4j


## About Python Environment
I have created a python environment *neo4j* where all the required modules are installed.
So before running any python file you need to activate this environment
```bash
# To activate the environment
source neo4j/bin/activate

# To deactivate the environment
deactivate
```


## About The Files
### myQueryExample.cypher
It contain some example of cypher queries

### load_database.cypher
It contain cypher queries for loading the database
```cypher
<!-- copy and paste queries and then run to execute them -->
```

### load_database.py
It contain a class which handle all the loading part of database using **neo4j module**
It provide following features:
* It connect to Neo4j Database
* We can run time change database folder
* Load Database
* Delete Whole Database
* Count the number of nodes
* Can generate statements for any type of data, we just need to provide data with header

### main.py
It uses load_database.py file for doing many things. It just like an application.
```bash
# <database folder> = Path to database in which all the .csv file exists

python main.py <database folder>
```
### other load_*.py
They also uses load_database.py file for loading database but in distributed way. Like just upload particular number of elements from a particular numbers
```bash
# For following files only
# load_nodes.py
# load_fields.py
# load_taxonomy_terms.py
# load_field_collection_items.py

# <filename> = any above file
# <database folder> = Path to database in which all the .csv file exists
# <start with> = integer which tell from where to start the loading
# <max count> = Which tell how many items/rows/nodes/etc are to be loaded

python <filename> <Database Folder> <start with> <max count>
```

## About Neo4j
**If you want to import data from csv file then do following changes in neo4j.config file**
dbms.security.allow_csv_import_from_file_urls=true  => UNCOMMENT IT
dbms.directories.import=import                      => COMMENT IT

**Installing APOC**
1. create a folder <NEO4J_HOME>/plugins
2. Download .jar file with the same version as your neo4j in <NEO4J_HOME>/plugins folder
3. change the following in the file
4. 
5. 

**Backup your database**
backup: Copy <NEO4J_HOME>/data/databases/graph.db folder
restore: replace <NEO4J_HOME>/data/databases/graph.db with your back *.db folder