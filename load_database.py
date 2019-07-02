from neo4j import GraphDatabase

class DatabaseHandler(object):
    def __init__(self, uri, user, password, dir='../database/ngov2/'):
        self.__change_dir__(dir)
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self._driver.close()

    def __change_dir__(self, dir):
        self.database_dir = dir # dir='file:///home/krrishna/Workspace/NPBridge/database/drupal/'
        return

    def return_head_tail(self, filename):
        with open(filename) as csv_file:
            import csv
            csv_reader = csv.reader(csv_file)
            csv_header = []
            for row in csv_reader:
                csv_header = row
                csv_header[0]=csv_header[0].replace('\ufeff', '')
                break
            csv_tail = []
            for (index, row) in enumerate(csv_reader):
                csv_tail.append(row)
        return csv_header, csv_tail

    def _write_transaction_(self, statement):
        return self._driver.session().write_transaction(lambda tx: tx.run(statement))

    # def load_database(self):
    #     try:
    #         self.load_nodes()
    #         self.load_field_collection_item()
    #         self.load_taxonomy_term()
    #         self.load_fields()
    #         print('Database succssfully Loaded')
    #         return True
    #     except Exception as e:
    #         print(e)
    #     return False

    def delete_whole_database(self):
        a = input('Are you sure about it(y/N): ')
        if(a != 'y' and a != 'Y'):
            print('Cancled')
            return False
        try:
            self._write_transaction_("MATCH (n) DETACH DELETE n")
            print('Whole Database Successfully Deleted')
            return True
        except Exception as e:
            print(e)
        return False

    def count_nodes(self):
        return self._write_transaction_("MATCH (n) return count(n)").single().value()

############################################################################################################
    def create_field_collection_item_statement(self, x='x', indent=0, node='node'):
        return \
            '\t'*indent + 'MERGE (' + node + ':field_collection_item{\n' + \
            '\t'*indent + '\titem_id: toInteger(' + x + '.item_id),\n' + \
            '\t'*indent + '\trevision_id: toInteger(' + x + '.revision_id),\n' + \
            '\t'*indent + '\tfield_name: ' + x + '.field_name,\n' + \
            '\t'*indent + '\tarchived: toInteger(' + x + '.archived)\n' + \
            '\t'*indent + '})'

    def load_field_collection_item_statement(self, x='x', indent=0, node='node'):
        return \
            '\t'*indent + 'LOAD CSV WITH HEADERS FROM \'' + self.database_dir + 'field_collection_item.csv\' AS ' + x + '\n' + \
            self.create_field_collection_item_statement(x, indent, node) + '\n' + \
            '\t'*indent + 'RETURN ' + node
    
    def load_field_collection_item(self):
        try:
            with self._driver.session() as session:
                session.write_transaction(lambda tx: tx.run(self.load_field_collection_item_statement()))
            print('Field Collection Item Successful Loaded')
            return True
        except Exception as e:
            print(e)
        return False

        

##################################3#################################################################################################33
    def load_fields(self):
        try:
            with self._driver.session() as session:
                session.write_transaction(lambda tx: tx.run(
                    "USING PERIODIC COMMIT 500 "
                    "LOAD CSV WITH HEADERS FROM 'file:///home/krrishna/Workspace/NPBridge/neo4j/ngov2/field_config.csv' AS x "
                    "WITH x.field_name as field_names, x.type as field_types "
                    "UNWIND field_names as field_name "
                    "LOAD CSV WITH HEADERS FROM 'file:///home/krrishna/Workspace/NPBridge/neo4j/ngov2/field_data_'+field_name+'.csv' AS x "
                    "match (n) where x.entity_type in labels(n) and "
                    "case "
                        "when exists(n.nid) then toInteger(n.nid) "
                        "when exists(n.item_id) then toInteger(n.item_id) "
                        "when exists(n.tid) then toInteger(n.tid) "
                        "else -1 "
                    "end = toInteger(x.entity_id) "

                    "CALL apoc.do.case([ "
                        "field_types = 'entityreference', \"MATCH (y:node) WHERE toInteger(y.nid) = toInteger(x[field_name+'_target_id']) RETURN y\", "
                        "field_types = 'taxonomy_term_reference', \"MATCH (y:taxonomy_term) WHERE toInteger(y.tid) = toInteger(x[field_name+'_tid']) RETURN y\", "
                        "field_types = 'field_collection', \"MATCH (y:field_collection_item) WHERE toInteger(y.item_id) = toInteger(x[field_name+'_value']) RETURN y\", "
                        "field_types = 'text_with_summary', \"MERGE(y:text_with_summary{ entity_id: toInteger(x.entity_id), entity_type: x.entity_type, bundle: x.bundle, deleted: toInteger(x.deleted), language: x.language, revision_id: toInteger(x.revision_id), delta: toInteger(x.delta), value: coalesce(x[field_name+'_value'], ''), summary: coalesce(x[field_name+'_summary'], ''), format: coalesce(x[field_name+'_format'], '') }) RETURN y\", "
                        "field_types = 'text_long', \"MERGE(y:text_long{ entity_id: toInteger(x.entity_id), entity_type: x.entity_type, bundle: x.bundle, deleted: toInteger(x.deleted), language: x.language, revision_id: toInteger(x.revision_id), delta: toInteger(x.delta), value: coalesce(x[field_name+'_value'], ''), format: coalesce(x[field_name+'_format'], '') }) RETURN y\", "
                        "field_types = 'image', \"MERGE(y:image{ entity_id: toInteger(x.entity_id), entity_type: x.entity_type, bundle: x.bundle, deleted: toInteger(x.deleted), language: x.language, revision_id: toInteger(x.revision_id), delta: toInteger(x.delta), title: coalesce(x[field_name+'_title'], ''), alt: coalesce(x[field_name+'_alt'], ''), fid: toFloat(x[field_name+'_fid']), width: toFloat(x[field_name+'_width']), height: toFloat(x[field_name+'_height']) }) RETURN y\", "
                        "field_types = 'list_text', \"MERGE(y:list_text{ entity_id: toInteger(x.entity_id), entity_type: x.entity_type, bundle: x.bundle, deleted: toInteger(x.deleted), language: x.language, revision_id: toInteger(x.revision_id), delta: toInteger(x.delta), value: coalesce(x[field_name+'_value'], '') }) RETURN y\", "
                        "field_types = 'text', \"MERGE(y:text{ entity_id: toInteger(x.entity_id), entity_type: x.entity_type, bundle: x.bundle, deleted: toInteger(x.deleted), language: x.language, revision_id: toInteger(x.revision_id), delta: toInteger(x.delta), value: coalesce(x[field_name+'_value'], ''), format: coalesce(x[field_name+'_format'], '') }) RETURN y\", "
                        "field_types = 'mobile_number', \"MERGE(y:mobile_number{ entity_id: toInteger(x.entity_id), entity_type: x.entity_type, bundle: x.bundle, deleted: toInteger(x.deleted), language: x.language, revision_id: toInteger(x.revision_id), delta: toInteger(x.delta), value: coalesce(x[field_name+'_value'], ''), country: coalesce(x[field_name+'_country'], ''), local_number: coalesce(x[field_name+'_local_number'], ''), verified: toInteger(x[field_name+'_verified']), tfa: toInteger(x[field_name+'_tfa']) }) RETURN y\", "
                        "field_types = 'email', \"MERGE(y:email{ entity_id: toInteger(x.entity_id), entity_type: x.entity_type, bundle: x.bundle, deleted: toInteger(x.deleted), language: x.language, revision_id: toInteger(x.revision_id), delta: toInteger(x.delta), email: coalesce(x[field_name+'_email'], '') }) RETURN y\", "
                        "field_types = 'geofield', \"MERGE(y:geofield{ entity_id: toInteger(x.entity_id), entity_type: x.entity_type, bundle: x.bundle, deleted: toInteger(x.deleted), language: x.language, revision_id: toInteger(x.revision_id), delta: toInteger(x.delta), geom: coalesce(x[field_name+'_geom'], ''), geo_type: coalesce(x[field_name+'_geo_type'], ''), lat: toFloat(x[field_name+'_lat']), lon: toFloat(x[field_name+'_lon']), left: toFloat(x[field_name+'_left']), right: toFloat(x[field_name+'_right']), top: toFloat(x[field_name+'_top']), bottom: toFloat(x[field_name+'_bottom']), geohash: coalesce(x[field_name+'_geohash'], '') }) RETURN y\", "
                        "field_types = 'number_integer', \"MERGE(y:number_integer{ entity_id: toInteger(x.entity_id), entity_type: x.entity_type, bundle: x.bundle, deleted: toInteger(x.deleted), language: x.language, revision_id: toInteger(x.revision_id), delta: toInteger(x.delta), value: toInteger(x[field_name+'_value']) }) RETURN y\", "
                        "field_types = 'link_field', \"MERGE(y:link_field{ entity_id: toInteger(x.entity_id), entity_type: x.entity_type, bundle: x.bundle, deleted: toInteger(x.deleted), language: x.language, revision_id: toInteger(x.revision_id), delta: toInteger(x.delta), url: coalesce(x[field_name+'_url'], ''), title: coalesce(x[field_name+'_title'], ''), attributes: coalesce(x[field_name+'_attributes'], '') }) RETURN y\" "
                        "], \"RETURN NULL\",{field_name:field_name, x:x} "
                    ") YIELD value "
                    "WITH field_name, n, value.y AS y WHERE y IS NOT NULL "

                    "CALL apoc.merge.relationship.eager(n, field_name, {},{},y,{}) YIELD rel as result "
                    "return n,y "
                ))
            print('Fields Successful Loaded')
            return True
        except Exception as e:
            print(e)
        return False

######################################################################################################################

    def csv_load_node(self, header, x, indent=0, labels='node'):
        statement = \
            '\t'*indent + 'MERGE (node:' + labels + '{\n' + \
            '\t'*indent + '\tnid: toInteger("' + x[header.index('nid')] +'"),\n' + \
            '\t'*indent + '\tvid: toInteger("' + x[header.index('vid')] +'"),\n' + \
            '\t'*indent + '\ttype: "' + x[header.index('type')].replace('"', '\\"') +'",\n' + \
            '\t'*indent + '\ttitle: "' + x[header.index('title')].replace('"', '\\"') +'",\n' + \
            '\t'*indent + '\tlanguage: "' + x[header.index('language')].replace('"', '\\"') +'",\n' + \
            '\t'*indent + '\tuid: toInteger("' + x[header.index('uid')] +'"),\n' + \
            '\t'*indent + '\tstatus: toInteger("' + x[header.index('status')] +'"),\n' + \
            '\t'*indent + '\tcreated: toInteger("' + x[header.index('created')] +'"),\n' + \
            '\t'*indent + '\tchanged: toInteger("' + x[header.index('changed')] +'"),\n' + \
            '\t'*indent + '\tcomment: toInteger("' + x[header.index('comment')] +'"),\n' + \
            '\t'*indent + '\tpromote: toInteger("' + x[header.index('promote')] +'"),\n' + \
            '\t'*indent + '\tsticky: toInteger("' + x[header.index('sticky')] +'"),\n' + \
            '\t'*indent + '\ttnid: toInteger("' + x[header.index('tnid')] +'"),\n' + \
            '\t'*indent + '\ttranslate: toInteger("' + x[header.index('translate')] +'")\n' + \
            '\t'*indent + '})'
        return statement
            
    def csv_load_nodes(self, x='x', indent=0, filename='node'):
        try:
            if(filename == 'node'):
                filename = self.database_dir + filename + '.csv'

            csv_header, rows = self.return_head_tail(filename)
            for i,row in enumerate(rows):
                print(i)
                statement = self.csv_load_node(csv_header, row)
                self._write_transaction_(statement)

            print('Nodes Successful Loaded')
            return True
        except Exception as e:
            print(e)
        return False

######################################################################################################################

    def csv_load_taxonomy_term(self, header, x, indent=0, labels='taxonomy_term'):
        statement = \
            '\t'*indent + 'MERGE (term:' + labels + '{\n' + \
            '\t'*indent + '\ttid: toInteger("' + x[header.index('tid')] +'"),\n' + \
            '\t'*indent + '\tvid: toInteger("' + x[header.index('vid')] +'"),\n' + \
            '\t'*indent + '\tname: "' + x[header.index('name')].replace('"', '\\"') +'",\n' + \
            '\t'*indent + '\tdescription: "' + x[header.index('description')].replace('"', '\\"') +'",\n' + \
            '\t'*indent + '\tformat: "' + x[header.index('format')].replace('"', '\\"') +'",\n' + \
            '\t'*indent + '\tweight: toInteger("' + x[header.index('weight')] +'"),\n' + \
            '\t'*indent + '})'
        return statement
                
    def csv_load_taxonomy_terms(self, x='x', indent=0, filename='taxonomy_term_data'):
        try:
            if(filename == 'taxonomy_term_data'):
                filename = self.database_dir + filename + '.csv'

            csv_header, rows = self.return_head_tail(filename)
            for i,row in enumerate(rows):
                print(i)
                statement = self.csv_load_taxonomy_term(csv_header, row)
                self._write_transaction_(statement)

            print('Taxonomy Terms Successful Loaded')
            return True
        except Exception as e:
            print(e)
        return False


############################################################################################################################

    def csv_load_field_collection_item(self, header, x, indent=0, labels='field_collection_item'):
        statement = \
            '\t'*indent + 'MERGE (item:' + labels + '{\n' + \
            '\t'*indent + '\titem_id: toInteger("' + x[header.index('item_id')] +'"),\n' + \
            '\t'*indent + '\trevision_id: toInteger("' + x[header.index('revision_id')] +'"),\n' + \
            '\t'*indent + '\tfield_name: "' + x[header.index('field_name')].replace('"', '\\"') +'",\n' + \
            '\t'*indent + '\tarchived: toInteger("' + x[header.index('archived')] +'"),\n' + \
            '\t'*indent + '})'
        return statement
            
    def csv_load_field_collection_items(self, x='x', indent=0, filename='field_collection_item'):
        try:
            if(filename == 'field_collection_item'):
                filename = self.database_dir + filename + '.csv'

            csv_header, rows = self.return_head_tail(filename)
            for i,row in enumerate(rows):
                print(i)
                statement = self.csv_load_taxonomy_term(csv_header, row)
                self._write_transaction_(statement)

            print('Field Collection Items Successful Loaded')
            return True
        except Exception as e:
            print(e)
        return False