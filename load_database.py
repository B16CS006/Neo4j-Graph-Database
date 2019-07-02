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

        if(filename.split('.')[-1] != 'csv'):
            filename = self.database_dir + filename + '.csv'

        # if(filename == 'node'):
        #     filename = self.database_dir + filename + '.csv'
        # elif(filename == 'taxonomy_term_data'):
        #     filename = self.database_dir + filename + '.csv'
        # elif(filename == 'field_collection_item'):
        #     filename = self.database_dir + filename + '.csv'

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

    def load_database(self):
        _nodes = self.csv_load_nodes()
        _taxonomy_terms = self.csv_load_taxonomy_terms()
        _field_collection_items = self.csv_load_field_collection_items()
        #_fields = self.csv_load_fields()
    
        if(_nodes and _taxonomy_terms and _field_collection_items):
            print('Database succssfully Loaded')
            return True
        else:
            return False

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

    def node_structure(self, header, x, indent=0, labels='node'):
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
            csv_header, rows = self.return_head_tail(filename)
            for i,row in enumerate(rows):
                print(i)
                statement = self.node_structure(csv_header, row)
                self._write_transaction_(statement)

            print('Nodes Successful Loaded')
            return True
        except Exception as e:
            print(e)
        return False

######################################################################################################################

    def taxonomy_term_structure(self, header, x, indent=0, labels='taxonomy_term'):
        statement = \
            '\t'*indent + 'MERGE (term:' + labels + '{\n' + \
            '\t'*indent + '\ttid: toInteger("' + x[header.index('tid')] +'"),\n' + \
            '\t'*indent + '\tvid: toInteger("' + x[header.index('vid')] +'"),\n' + \
            '\t'*indent + '\tname: "' + x[header.index('name')].replace('"', '\\"') +'",\n' + \
            '\t'*indent + '\tdescription: "' + x[header.index('description')].replace('"', '\\"') +'",\n' + \
            '\t'*indent + '\tformat: "' + x[header.index('format')].replace('"', '\\"') +'",\n' + \
            '\t'*indent + '\tweight: toInteger("' + x[header.index('weight')] +'")\n' + \
            '\t'*indent + '})'
        return statement
                
    def csv_load_taxonomy_terms(self, x='x', indent=0, filename='taxonomy_term_data'):
        try:
            csv_header, rows = self.return_head_tail(filename)
            for i,row in enumerate(rows):
                print(i)
                statement = self.taxonomy_term_structure(csv_header, row)
                self._write_transaction_(statement)

            print('Taxonomy Terms Successful Loaded')
            return True
        except Exception as e:
            print(e)
        return False


############################################################################################################################

    def field_collection_item_structure(self, header, x, indent=0, labels='field_collection_item'):
        statement = \
            '\t'*indent + 'MERGE (item:' + labels + '{\n' + \
            '\t'*indent + '\titem_id: toInteger("' + x[header.index('item_id')] +'"),\n' + \
            '\t'*indent + '\trevision_id: toInteger("' + x[header.index('revision_id')] +'"),\n' + \
            '\t'*indent + '\tfield_name: "' + x[header.index('field_name')].replace('"', '\\"') +'",\n' + \
            '\t'*indent + '\tarchived: toInteger("' + x[header.index('archived')] +'")\n' + \
            '\t'*indent + '})'
        return statement
            
    def csv_load_field_collection_items(self, x='x', indent=0, filename='field_collection_item'):
        try:
            csv_header, rows = self.return_head_tail(filename)
            for i,row in enumerate(rows):
                print(i)
                statement = self.field_collection_item_structure(csv_header, row)
                self._write_transaction_(statement)

            print('Field Collection Items Successful Loaded')
            return True
        except Exception as e:
            print(e)
        return False

#########################################################################################################################

    def csv_load_fields(self):
        field_config_header, field_config_tail = self.return_head_tail('field_config')
        for field_config_element in field_config_tail:
            field_name = field_config_element[field_config_header.index('field_name')]
            field_type = field_config_element[field_config_header.index('type')]
            print('\n', field_name, field_type)
            if(field_name == 'field_geofield' or field_name == 'comment_body'):
                continue
            field_data_header, field_data_tail = self.return_head_tail('field_data_' + field_name)
            # print(field_data_header)
            # print(field_data_tail[0])
                
            for field_data_element in field_data_tail:
                entity_type = field_data_element[field_data_header.index('entity_type')]
                entity_id = field_data_element[field_data_header.index('entity_id')]

                statement = 'MATCH (n:' + entity_type + ') WHERE toInteger(n.'

                if(entity_type == 'node'):
                    statement = statement + 'nid'
                elif(entity_type == 'field_collection_item'):
                    statement = statement + 'item_id'
                elif(entity_type == 'taxonomy_term'):
                    statement = statement + 'tid'
                elif(entity_type == 'comment'):
                    continue

                statement = statement + ') = toInteger("' + entity_id + '")\n'
                
                # print('/////////////////////////////;')
                # print(field_config_header)
                # print(field_config_element,'\n')
                # print(field_data_header,'\n')
                # print(field_data_element)
                # return

                print('field_' + field_type + '_structure'(field_data_header, field_data_element) + '\n')
                # statement = statement + 'field_' + field_type + '_structure'(field_data_header, field_data_element) + '\n'
                
                # print(statement,'\n')

        

    # def load_fields(self):
    #     try:
    #         with self._driver.session() as session:
    #             session.write_transaction(lambda tx: tx.run(
    #                 "USING PERIODIC COMMIT 500 "
    #                 "LOAD CSV WITH HEADERS FROM 'file:///home/krrishna/Workspace/NPBridge/neo4j/ngov2/field_config.csv' AS x "
    #                 "WITH x.field_name as field_names, x.type as field_types "
    #                 "UNWIND field_names as field_name "
    #                 "LOAD CSV WITH HEADERS FROM 'file:///home/krrishna/Workspace/NPBridge/neo4j/ngov2/field_data_'+field_name+'.csv' AS x "
    #                 "match (n) where x.entity_type in labels(n) and "
    #                 "case "
    #                     "when exists(n.nid) then toInteger(n.nid) "
    #                     "when exists(n.item_id) then toInteger(n.item_id) "
    #                     "when exists(n.tid) then toInteger(n.tid) "
    #                     "else -1 "
    #                 "end = toInteger(x.entity_id) "

    #                 "CALL apoc.do.case([ "
    #                     "field_types = 'entityreference', \"MATCH (y:node) WHERE toInteger(y.nid) = toInteger(x[field_name+'_target_id']) RETURN y\", "
    #                     "field_types = 'taxonomy_term_reference', \"MATCH (y:taxonomy_term) WHERE toInteger(y.tid) = toInteger(x[field_name+'_tid']) RETURN y\", "
    #                     "field_types = 'field_collection', \"MATCH (y:field_collection_item) WHERE toInteger(y.item_id) = toInteger(x[field_name+'_value']) RETURN y\", "
    #                     "field_types = 'text_with_summary', \"MERGE(y:text_with_summary{ entity_id: toInteger(x.entity_id), entity_type: x.entity_type, bundle: x.bundle, deleted: toInteger(x.deleted), language: x.language, revision_id: toInteger(x.revision_id), delta: toInteger(x.delta), value: coalesce(x[field_name+'_value'], ''), summary: coalesce(x[field_name+'_summary'], ''), format: coalesce(x[field_name+'_format'], '') }) RETURN y\", "
    #                     "field_types = 'text_long', \"MERGE(y:text_long{ entity_id: toInteger(x.entity_id), entity_type: x.entity_type, bundle: x.bundle, deleted: toInteger(x.deleted), language: x.language, revision_id: toInteger(x.revision_id), delta: toInteger(x.delta), value: coalesce(x[field_name+'_value'], ''), format: coalesce(x[field_name+'_format'], '') }) RETURN y\", "
    #                     "field_types = 'image', \"MERGE(y:image{ entity_id: toInteger(x.entity_id), entity_type: x.entity_type, bundle: x.bundle, deleted: toInteger(x.deleted), language: x.language, revision_id: toInteger(x.revision_id), delta: toInteger(x.delta), title: coalesce(x[field_name+'_title'], ''), alt: coalesce(x[field_name+'_alt'], ''), fid: toFloat(x[field_name+'_fid']), width: toFloat(x[field_name+'_width']), height: toFloat(x[field_name+'_height']) }) RETURN y\", "
    #                     "field_types = 'list_text', \"MERGE(y:list_text{ entity_id: toInteger(x.entity_id), entity_type: x.entity_type, bundle: x.bundle, deleted: toInteger(x.deleted), language: x.language, revision_id: toInteger(x.revision_id), delta: toInteger(x.delta), value: coalesce(x[field_name+'_value'], '') }) RETURN y\", "
    #                     "field_types = 'text', \"MERGE(y:text{ entity_id: toInteger(x.entity_id), entity_type: x.entity_type, bundle: x.bundle, deleted: toInteger(x.deleted), language: x.language, revision_id: toInteger(x.revision_id), delta: toInteger(x.delta), value: coalesce(x[field_name+'_value'], ''), format: coalesce(x[field_name+'_format'], '') }) RETURN y\", "
    #                     "field_types = 'mobile_number', \"MERGE(y:mobile_number{ entity_id: toInteger(x.entity_id), entity_type: x.entity_type, bundle: x.bundle, deleted: toInteger(x.deleted), language: x.language, revision_id: toInteger(x.revision_id), delta: toInteger(x.delta), value: coalesce(x[field_name+'_value'], ''), country: coalesce(x[field_name+'_country'], ''), local_number: coalesce(x[field_name+'_local_number'], ''), verified: toInteger(x[field_name+'_verified']), tfa: toInteger(x[field_name+'_tfa']) }) RETURN y\", "
    #                     "field_types = 'email', \"MERGE(y:email{ entity_id: toInteger(x.entity_id), entity_type: x.entity_type, bundle: x.bundle, deleted: toInteger(x.deleted), language: x.language, revision_id: toInteger(x.revision_id), delta: toInteger(x.delta), email: coalesce(x[field_name+'_email'], '') }) RETURN y\", "
    #                     "field_types = 'geofield', \"MERGE(y:geofield{ entity_id: toInteger(x.entity_id), entity_type: x.entity_type, bundle: x.bundle, deleted: toInteger(x.deleted), language: x.language, revision_id: toInteger(x.revision_id), delta: toInteger(x.delta), geom: coalesce(x[field_name+'_geom'], ''), geo_type: coalesce(x[field_name+'_geo_type'], ''), lat: toFloat(x[field_name+'_lat']), lon: toFloat(x[field_name+'_lon']), left: toFloat(x[field_name+'_left']), right: toFloat(x[field_name+'_right']), top: toFloat(x[field_name+'_top']), bottom: toFloat(x[field_name+'_bottom']), geohash: coalesce(x[field_name+'_geohash'], '') }) RETURN y\", "
    #                     "field_types = 'number_integer', \"MERGE(y:number_integer{ entity_id: toInteger(x.entity_id), entity_type: x.entity_type, bundle: x.bundle, deleted: toInteger(x.deleted), language: x.language, revision_id: toInteger(x.revision_id), delta: toInteger(x.delta), value: toInteger(x[field_name+'_value']) }) RETURN y\", "
    #                     "field_types = 'link_field', \"MERGE(y:link_field{ entity_id: toInteger(x.entity_id), entity_type: x.entity_type, bundle: x.bundle, deleted: toInteger(x.deleted), language: x.language, revision_id: toInteger(x.revision_id), delta: toInteger(x.delta), url: coalesce(x[field_name+'_url'], ''), title: coalesce(x[field_name+'_title'], ''), attributes: coalesce(x[field_name+'_attributes'], '') }) RETURN y\" "
    #                     "], \"RETURN NULL\",{field_name:field_name, x:x} "
    #                 ") YIELD value "
    #                 "WITH field_name, n, value.y AS y WHERE y IS NOT NULL "

    #                 "CALL apoc.merge.relationship.eager(n, field_name, {},{},y,{}) YIELD rel as result "
    #                 "return n,y "
    #             ))
    #         print('Fields Successful Loaded')
    #         return True
    #     except Exception as e:
    #         print(e)
    #     return False



################################################################################################

    def __column_with_string__(column_name, column_value, indent = 0):
        return '\t'*indent + '\t' + column_name + ': "' + column_value.replace('"', '\\"') + '"'

    def __column_with_integer__(column_name, column_value, indent = 0):
        return '\t'*indent + '\t' + column_name + ': toInteger("' + column_value + '")'

    def __column_with_string_and_field_name__(column_name, column_value, indent = 0):
        return '\t'*indent + '\t' + column_name + ': "' + column_value.replace('"', '\\"') + '"'

    def __column_with_integer_and_field_name__(column_name, column_value, indent = 0):
        return '\t'*indent + '\t' + column_name + ': toInteger("' + column_value + '")'

    def __column_with_float_and_field_name__(column_name, column_value, indent = 0):
        return '\t'*indent + '\t' + column_name + ': toFloat("' + column_value + '")'

######################################################################################################

    def field_entityreference_structure(self, header, x, fieldname, indent=0):
        statement = \
            '\t'*indent + 'MATCH (y :node) ' + \
            '\t'*indent + 'WHERE toInteger(y.nid) = toInteger(("' + x[header.index(fieldname + '_target_id')] +'")\n' + \
            '\t'*indent + '}) RETURN y'
        return statement

    def field_taxonomy_term_structure(self, header, x, fieldname, indent=0):
        statement = \
            '\t'*indent + 'MATCH (y :taxonomy_term) ' + \
            '\t'*indent + 'WHERE toInteger(y.tid) = toInteger(("' + x[header.index(fieldname + '_tid')] +'")\n' + \
            '\t'*indent + '}) RETURN y'
        return statement

    def field_field_collection_structure(self, header, x, fieldname, indent=0):
        statement = \
            '\t'*indent + 'MATCH (y :field_collection_item) ' + \
            '\t'*indent + 'WHERE toInteger(y.item_id) = toInteger(("' + x[header.index(fieldname + '_value')] +'")\n' + \
            '\t'*indent + '}) RETURN y'
        return statement

    def field_email_structure(self, header, x, indent=0):
        statement = \
            '\t'*indent + 'MERGE (y :email{\n' + \
            '\t'*indent + '\tentity_type: "' + x[header.index('entity_type')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tbundle: "' + x[header.index('bundle')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tdeleted: toInteger("' + x[header.index('deleted')] + '"),\n' + \
            '\t'*indent + '\tentity_id: toInteger("' + x[header.index('entity_id')] + '"),\n' + \
            '\t'*indent + '\trevision_id: toInteger("' + x[header.index('revision_id')] + '"),\n' + \
            '\t'*indent + '\tlanguage: "' + x[header.index('langauge')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tdelta: toInteger("' + x[header.index('delta')] + '"),\n' + \
            '\t'*indent + '\temail: "' + x[header.index(fieldname + '_email')].replace('"', '\\"') + '"\n' + \
            '\t'*indent + '}) RETURN y'
        return statement

    def field_text_with_summary_structure(self, header, x, indent=0):
        statement = \
            '\t'*indent + 'MERGE (y :text_with_summary{\n' + \
            '\t'*indent + '\tentity_type: "' + x[header.index('entity_type')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tbundle: "' + x[header.index('bundle')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tdeleted: toInteger("' + x[header.index('deleted')] + '"),\n' + \
            '\t'*indent + '\tentity_id: toInteger("' + x[header.index('entity_id')] + '"),\n' + \
            '\t'*indent + '\trevision_id: toInteger("' + x[header.index('revision_id')] + '"),\n' + \
            '\t'*indent + '\tlanguage: "' + x[header.index('langauge')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tdelta: toInteger("' + x[header.index('delta')] + '"),\n' + \
            '\t'*indent + '\tvalue: "' + x[header.index(fieldname + '_value')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tsummary: "' + x[header.index(fieldname + '_summary')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tformat: "' + x[header.index(fieldname + '_format')].replace('"', '\\"') + '"\n' + \
            '\t'*indent + '}) RETURN y'
        return statement

    def field_text_long_structure(self, header, x, indent=0):
        statement = \
            '\t'*indent + 'MERGE (y :text_long{\n' + \
            '\t'*indent + '\tentity_type: "' + x[header.index('entity_type')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tbundle: "' + x[header.index('bundle')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tdeleted: toInteger("' + x[header.index('deleted')] + '"),\n' + \
            '\t'*indent + '\tentity_id: toInteger("' + x[header.index('entity_id')] + '"),\n' + \
            '\t'*indent + '\trevision_id: toInteger("' + x[header.index('revision_id')] + '"),\n' + \
            '\t'*indent + '\tlanguage: "' + x[header.index('langauge')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tdelta: toInteger("' + x[header.index('delta')] + '"),\n' + \
            '\t'*indent + '\tvalue: "' + x[header.index(fieldname + '_value')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tformat: "' + x[header.index(fieldname + '_format')].replace('"', '\\"') + '"\n' + \
            '\t'*indent + '}) RETURN y'
        return statement

    def field_image_structure(self, header, x, indent=0):
        statement = \
            '\t'*indent + 'MERGE (y :image{\n' + \
            '\t'*indent + '\tentity_type: "' + x[header.index('entity_type')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tbundle: "' + x[header.index('bundle')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tdeleted: toInteger("' + x[header.index('deleted')] + '"),\n' + \
            '\t'*indent + '\tentity_id: toInteger("' + x[header.index('entity_id')] + '"),\n' + \
            '\t'*indent + '\trevision_id: toInteger("' + x[header.index('revision_id')] + '"),\n' + \
            '\t'*indent + '\tlanguage: "' + x[header.index('langauge')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tdelta: toInteger("' + x[header.index('delta')] + '"),\n' + \
            '\t'*indent + '\ttitle: "' + x[header.index(fieldname + '_title')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\talt: "' + x[header.index(fieldname + '_alt')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tfid: toInteger("' + x[header.index(filename + '_fid')] + '"),\n' + \
            '\t'*indent + '\twidth: toFloat("' + x[header.index(filename + '_width')] + '"),\n' + \
            '\t'*indent + '\theight: toFloat("' + x[header.index(filename + '_height')] + '")\n' + \
            '\t'*indent + '}) RETURN y'
        return statement

    def field_list_text_structure(self, header, x, indent=0):
        statement = \
            '\t'*indent + 'MERGE (y :list_text{\n' + \
            '\t'*indent + '\tentity_type: "' + x[header.index('entity_type')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tbundle: "' + x[header.index('bundle')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tdeleted: toInteger("' + x[header.index('deleted')] + '"),\n' + \
            '\t'*indent + '\tentity_id: toInteger("' + x[header.index('entity_id')] + '"),\n' + \
            '\t'*indent + '\trevision_id: toInteger("' + x[header.index('revision_id')] + '"),\n' + \
            '\t'*indent + '\tlanguage: "' + x[header.index('langauge')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tdelta: toInteger("' + x[header.index('delta')] + '"),\n' + \
            '\t'*indent + '\tvalue: "' + x[header.index(fieldname + '_value')].replace('"', '\\"') + '"\n' + \
            '\t'*indent + '}) RETURN y'
        return statement

    def field_text_structure(self, header, x, indent=0):
        statement = \
            '\t'*indent + 'MERGE (y :text{\n' + \
            '\t'*indent + '\tentity_type: "' + x[header.index('entity_type')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tbundle: "' + x[header.index('bundle')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tdeleted: toInteger("' + x[header.index('deleted')] + '"),\n' + \
            '\t'*indent + '\tentity_id: toInteger("' + x[header.index('entity_id')] + '"),\n' + \
            '\t'*indent + '\trevision_id: toInteger("' + x[header.index('revision_id')] + '"),\n' + \
            '\t'*indent + '\tlanguage: "' + x[header.index('langauge')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tdelta: toInteger("' + x[header.index('delta')] + '"),\n' + \
            '\t'*indent + '\tvalue: "' + x[header.index(fieldname + '_value')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tformat: "' + x[header.index(fieldname + '_format')].replace('"', '\\"') + '"\n' + \
            '\t'*indent + '}) RETURN y'
        return statement    

    def field_mobile_number_structure(self, header, x, indent=0):
        statement = \
            '\t'*indent + 'MERGE (y :mobile_number{\n' + \
            '\t'*indent + '\tentity_type: "' + x[header.index('entity_type')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tbundle: "' + x[header.index('bundle')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tdeleted: toInteger("' + x[header.index('deleted')] + '"),\n' + \
            '\t'*indent + '\tentity_id: toInteger("' + x[header.index('entity_id')] + '"),\n' + \
            '\t'*indent + '\trevision_id: toInteger("' + x[header.index('revision_id')] + '"),\n' + \
            '\t'*indent + '\tlanguage: "' + x[header.index('langauge')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tdelta: toInteger("' + x[header.index('delta')] + '"),\n' + \
            '\t'*indent + '\tvalue: "' + x[header.index(field_name + '_value')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tcountry: "' + x[header.index(field_name + '_country')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tlocal_number: "' + x[header.index(field_name + '_local_number')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tverified: toInteger("' + x[header.index(field_name+'_verified')] + '"),\n' + \
            '\t'*indent + '\ttfa: toInteger("' + x[header.index(field_name+'_tfa')] + '")\n' + \
            '\t'*indent + '}) RETURN y'
        return statement

    def field_geofield_structure(self, header, x, indent=0):
        statement = \
            '\t'*indent + 'MERGE (y :geofield{\n' + \
            '\t'*indent + '\tentity_type: "' + x[header.index('entity_type')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tbundle: "' + x[header.index('bundle')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tdeleted: toInteger("' + x[header.index('deleted')] + '"),\n' + \
            '\t'*indent + '\tentity_id: toInteger("' + x[header.index('entity_id')] + '"),\n' + \
            '\t'*indent + '\trevision_id: toInteger("' + x[header.index('revision_id')] + '"),\n' + \
            '\t'*indent + '\tlanguage: "' + x[header.index('langauge')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tdelta: toInteger("' + x[header.index('delta')] + '"),\n' + \
            '\t'*indent + '\tgeom: "' + x[header(field_name + '_geom')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tgeo_type: "' + x[header(field_name + '_geo_type')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tlat: toFloat("' + x[header.index(field_name+'_lat')] + '"),\n' + \
            '\t'*indent + '\tlon: toFloat("' + x[header.index(field_name+'_lon')] + '"),\n' + \
            '\t'*indent + '\tleft: toFloat("' + x[header.index(field_name+'_left')] + '"),\n' + \
            '\t'*indent + '\tright: toFloat("' + x[header.index(field_name+'_right')] + '"),\n' + \
            '\t'*indent + '\ttop: toFloat("' + x[header.index(field_name+'_top')] + '"),\n' + \
            '\t'*indent + '\tbottom: toFloat("' + x[header.index(field_name+'_bottom')] + '"),\n' + \
            '\t'*indent + '\tgeohash: "' + x[header(field_name + '_geohash')].replace('"', '\\"') + '"\n' + \
            '\t'*indent + '}) RETURN y'
        return statement

    def field_number_integer_structure(self, header, x, indent=0):
        statement = \
            '\t'*indent + 'MERGE (y :number_integer{\n' + \
            '\t'*indent + '\tentity_type: "' + x[header.index('entity_type')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tbundle: "' + x[header.index('bundle')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tdeleted: toInteger("' + x[header.index('deleted')] + '"),\n' + \
            '\t'*indent + '\tentity_id: toInteger("' + x[header.index('entity_id')] + '"),\n' + \
            '\t'*indent + '\trevision_id: toInteger("' + x[header.index('revision_id')] + '"),\n' + \
            '\t'*indent + '\tlanguage: "' + x[header.index('langauge')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tdelta: toInteger("' + x[header.index('delta')] + '"),\n' + \
            '\t'*indent + '\tvalue: toInteger("' + x[header.index(field_name+'_value')] + '")\n' + \
            '\t'*indent + '}) RETURN y'
        return statement

    def field_link_field_structure(self, header, x, indent=0):
        statement = \
            '\t'*indent + 'MERGE (y :link_field{\n' + \
            '\t'*indent + '\tentity_type: "' + x[header.index('entity_type')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tbundle: "' + x[header.index('bundle')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tdeleted: toInteger("' + x[header.index('deleted')] + '"),\n' + \
            '\t'*indent + '\tentity_id: toInteger("' + x[header.index('entity_id')] + '"),\n' + \
            '\t'*indent + '\trevision_id: toInteger("' + x[header.index('revision_id')] + '"),\n' + \
            '\t'*indent + '\tlanguage: "' + x[header.index('langauge')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tdelta: toInteger("' + x[header.index('delta')] + '"),\n' + \
            '\t'*indent + '\turl: "' + x[header(field_name + '_url')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\ttitle: "' + x[header(field_name + '_title')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tattributes: "' + x[header(field_name + '_attributes')].replace('"', '\\"') + '"\n' + \
            '\t'*indent + '}) RETURN y'
        return statement
    