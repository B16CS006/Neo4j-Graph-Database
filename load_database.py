from neo4j import GraphDatabase

class BreakIt(Exception):
    pass

class DatabaseHandler(object):
    def __init__(self, uri, user, password, dir='../database/ngov2/'):
        self.__change_dir__(dir)
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self._driver.close()

    def __change_dir__(self, dir):
        self.database_dir = dir # dir='file:///home/krrishna/Workspace/NPBridge/database/drupal/'
        return

    def __filename__(self, filename):
        if(filename.split('.')[-1] != 'csv'):
            filename = self.database_dir + filename + '.csv'
        return filename

    def count_lines_in_file(self, filename):
        return len(self.return_head_tail(filename)[1])
    
    def return_head_tail(self, filename):
        filename = self.__filename__(filename)
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
        _fields = self.csv_load_fields()
    
        if(_nodes and _taxonomy_terms and _field_collection_items and _fields):
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
    
######################################################################################################################
            
    def csv_load_nodes(self, start_from=0, max_count=-1, indent=0, filename='node'):
        try:
            csv_header, rows = self.return_head_tail(filename)
            if(max_count <0):
                rows = rows[start_from:]
            else:
                rows = rows[start_from: start_from + max_count]

            for i,row in enumerate(rows):
                if(max_count > 0 and i - start_from >= max_count):
                    print(i-start_from, max_count)
                    break
                print('i =', i, ', count = ', i + start_from)
                statement = self.node_structure(csv_header, row, indent)
                self._write_transaction_(statement)

            print('Nodes Successful Loaded')
            return True
        except Exception as e:
            print(e)
        return False

    def csv_load_taxonomy_terms(self, start_from=0, max_count=-1, indent=0, filename='taxonomy_term_data'):
        try:
            csv_header, rows = self.return_head_tail(filename)
            if(max_count <0):
                rows = rows[start_from:]
            else:
                rows = rows[start_from: start_from + max_count]

            for i,row in enumerate(rows):
                if(max_count > 0 and i - start_from >= max_count):
                    break
                print('i =', i, ', count = ', i + start_from)
                statement = self.taxonomy_term_structure(csv_header, row, indent)
                self._write_transaction_(statement)

            print('Taxonomy Terms Successful Loaded')
            return True
        except Exception as e:
            print(e)
        return False

    def csv_load_field_collection_items(self, start_from=0, max_count=-1, indent=0, filename='field_collection_item'):
        try:
            csv_header, rows = self.return_head_tail(filename)
            if(max_count <0):
                rows = rows[start_from:]
            else:
                rows = rows[start_from: start_from + max_count]
            for i,row in enumerate(rows):
                if(max_count > 0 and i - start_from >= max_count):
                    break
                print('i =', i, ', count = ', i + start_from)
                statement = self.field_collection_item_structure(csv_header, row, indent)
                self._write_transaction_(statement)

            print('Field Collection Items Successful Loaded')
            return True
        except Exception as e:
            print(e)
        return False

    def csv_load_fields(self, start_from=0, max_count=-1, indent=0):
        try:
            field_config_header, field_config_tail = self.return_head_tail('field_config')
            count = 0
            for i, field_config_element in enumerate(field_config_tail):
                field_name = field_config_element[field_config_header.index('field_name')]
                field_type = field_config_element[field_config_header.index('type')]
                # if(field_name == 'field_geofield' or field_name == 'comment_body'):
                    # continue
                field_data_header, field_data_tail = self.return_head_tail('field_data_' + field_name)
                
                if(count + len(field_data_tail) <= start_from):
                    count += len(field_data_tail)
                    field_data_header.clear()
                    field_data_tail.clear()
                    continue
                elif(max_count > 0 and count >= start_from + max_count):
                    break
                else:
                    start_index = max(0, start_from - count)
                    if(max_count < 0):
                        field_data_tail = field_data_tail[start_index:]
                    else:
                        field_data_tail = field_data_tail[start_index : start_from + max_count - count]
                    count += len(field_data_tail) + start_index
                    
                for j, field_data_element in enumerate(field_data_tail):
                    print('i =', i, ', j =', j, ', count = ', count)
                    # continue
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

                    function_name = 'field_' + field_type + '_structure'

                    statement = statement + ') = toInteger("' + entity_id + '")\nWITH n\n' + \
                        getattr(self, function_name)(field_data_header, field_data_element, field_name, indent) + '\n' + \
                        'MERGE (n)-[:' + field_name + ']->(y)'
                    self._write_transaction_(statement)
            print('Fields Successfully Loaded')
            return True
        except BreakIt as e:
            print(e)
            return True
        except Exception as e:
            print(e)
        return False

######################################################################################################

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

    def field_collection_item_structure(self, header, x, indent=0, labels='field_collection_item'):
        statement = \
            '\t'*indent + 'MERGE (item:' + labels + '{\n' + \
            '\t'*indent + '\titem_id: toInteger("' + x[header.index('item_id')] +'"),\n' + \
            '\t'*indent + '\trevision_id: toInteger("' + x[header.index('revision_id')] +'"),\n' + \
            '\t'*indent + '\tfield_name: "' + x[header.index('field_name')].replace('"', '\\"') +'",\n' + \
            '\t'*indent + '\tarchived: toInteger("' + x[header.index('archived')] +'")\n' + \
            '\t'*indent + '})'
        return statement

    def field_entityreference_structure(self, header, x, fieldname, indent=0):
        statement = \
            '\t'*indent + 'MATCH (y :node) ' + \
            '\t'*indent + 'WHERE toInteger(y.nid) = toInteger("' + x[header.index(fieldname + '_target_id')] +'")\n'
        return statement

    def field_taxonomy_term_reference_structure(self, header, x, fieldname, indent=0):
        statement = \
            '\t'*indent + 'MATCH (y :taxonomy_term) ' + \
            '\t'*indent + 'WHERE toInteger(y.tid) = toInteger("' + x[header.index(fieldname + '_tid')] +'")\n'
        return statement

    def field_field_collection_structure(self, header, x, fieldname, indent=0):
        statement = \
            '\t'*indent + 'MATCH (y :field_collection_item) ' + \
            '\t'*indent + 'WHERE toInteger(y.item_id) = toInteger("' + x[header.index(fieldname + '_value')] +'")\n'
        return statement

    def field_email_structure(self, header, x, fieldname, indent=0):
        statement = \
            '\t'*indent + 'MERGE (y :email{\n' + \
            '\t'*indent + '\tentity_type: "' + x[header.index('entity_type')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tbundle: "' + x[header.index('bundle')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tdeleted: toInteger("' + x[header.index('deleted')] + '"),\n' + \
            '\t'*indent + '\tentity_id: toInteger("' + x[header.index('entity_id')] + '"),\n' + \
            '\t'*indent + '\trevision_id: toInteger("' + x[header.index('revision_id')] + '"),\n' + \
            '\t'*indent + '\tlanguage: "' + x[header.index('language')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tdelta: toInteger("' + x[header.index('delta')] + '"),\n' + \
            '\t'*indent + '\temail: "' + x[header.index(fieldname + '_email')].replace('"', '\\"') + '"\n' + \
            '\t'*indent + '})'
        return statement

    def field_text_with_summary_structure(self, header, x, fieldname, indent=0):
        statement = \
            '\t'*indent + 'MERGE (y :text_with_summary{\n' + \
            '\t'*indent + '\tentity_type: "' + x[header.index('entity_type')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tbundle: "' + x[header.index('bundle')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tdeleted: toInteger("' + x[header.index('deleted')] + '"),\n' + \
            '\t'*indent + '\tentity_id: toInteger("' + x[header.index('entity_id')] + '"),\n' + \
            '\t'*indent + '\trevision_id: toInteger("' + x[header.index('revision_id')] + '"),\n' + \
            '\t'*indent + '\tlanguage: "' + x[header.index('language')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tdelta: toInteger("' + x[header.index('delta')] + '"),\n' + \
            '\t'*indent + '\tvalue: "' + x[header.index(fieldname + '_value')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tsummary: "' + x[header.index(fieldname + '_summary')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tformat: "' + x[header.index(fieldname + '_format')].replace('"', '\\"') + '"\n' + \
            '\t'*indent + '})'
        return statement

    def field_text_long_structure(self, header, x, fieldname, indent=0):
        statement = \
            '\t'*indent + 'MERGE (y :text_long{\n' + \
            '\t'*indent + '\tentity_type: "' + x[header.index('entity_type')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tbundle: "' + x[header.index('bundle')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tdeleted: toInteger("' + x[header.index('deleted')] + '"),\n' + \
            '\t'*indent + '\tentity_id: toInteger("' + x[header.index('entity_id')] + '"),\n' + \
            '\t'*indent + '\trevision_id: toInteger("' + x[header.index('revision_id')] + '"),\n' + \
            '\t'*indent + '\tlanguage: "' + x[header.index('language')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tdelta: toInteger("' + x[header.index('delta')] + '"),\n' + \
            '\t'*indent + '\tvalue: "' + x[header.index(fieldname + '_value')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tformat: "' + x[header.index(fieldname + '_format')].replace('"', '\\"') + '"\n' + \
            '\t'*indent + '})'
        return statement

    def field_image_structure(self, header, x, fieldname, indent=0):
        statement = \
            '\t'*indent + 'MERGE (y :image{\n' + \
            '\t'*indent + '\tentity_type: "' + x[header.index('entity_type')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tbundle: "' + x[header.index('bundle')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tdeleted: toInteger("' + x[header.index('deleted')] + '"),\n' + \
            '\t'*indent + '\tentity_id: toInteger("' + x[header.index('entity_id')] + '"),\n' + \
            '\t'*indent + '\trevision_id: toInteger("' + x[header.index('revision_id')] + '"),\n' + \
            '\t'*indent + '\tlanguage: "' + x[header.index('language')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tdelta: toInteger("' + x[header.index('delta')] + '"),\n' + \
            '\t'*indent + '\ttitle: "' + x[header.index(fieldname + '_title')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\talt: "' + x[header.index(fieldname + '_alt')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tfid: toInteger("' + x[header.index(fieldname + '_fid')] + '"),\n' + \
            '\t'*indent + '\twidth: toFloat("' + x[header.index(fieldname + '_width')] + '"),\n' + \
            '\t'*indent + '\theight: toFloat("' + x[header.index(fieldname + '_height')] + '")\n' + \
            '\t'*indent + '})'
        return statement

    def field_list_text_structure(self, header, x, fieldname, indent=0):
        statement = \
            '\t'*indent + 'MERGE (y :list_text{\n' + \
            '\t'*indent + '\tentity_type: "' + x[header.index('entity_type')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tbundle: "' + x[header.index('bundle')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tdeleted: toInteger("' + x[header.index('deleted')] + '"),\n' + \
            '\t'*indent + '\tentity_id: toInteger("' + x[header.index('entity_id')] + '"),\n' + \
            '\t'*indent + '\trevision_id: toInteger("' + x[header.index('revision_id')] + '"),\n' + \
            '\t'*indent + '\tlanguage: "' + x[header.index('language')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tdelta: toInteger("' + x[header.index('delta')] + '"),\n' + \
            '\t'*indent + '\tvalue: "' + x[header.index(fieldname + '_value')].replace('"', '\\"') + '"\n' + \
            '\t'*indent + '})'
        return statement

    def field_text_structure(self, header, x, fieldname, indent=0):
        statement = \
            '\t'*indent + 'MERGE (y :text{\n' + \
            '\t'*indent + '\tentity_type: "' + x[header.index('entity_type')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tbundle: "' + x[header.index('bundle')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tdeleted: toInteger("' + x[header.index('deleted')] + '"),\n' + \
            '\t'*indent + '\tentity_id: toInteger("' + x[header.index('entity_id')] + '"),\n' + \
            '\t'*indent + '\trevision_id: toInteger("' + x[header.index('revision_id')] + '"),\n' + \
            '\t'*indent + '\tlanguage: "' + x[header.index('language')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tdelta: toInteger("' + x[header.index('delta')] + '"),\n' + \
            '\t'*indent + '\tvalue: "' + x[header.index(fieldname + '_value')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tformat: "' + x[header.index(fieldname + '_format')].replace('"', '\\"') + '"\n' + \
            '\t'*indent + '})'
        return statement    

    def field_mobile_number_structure(self, header, x, fieldname, indent=0):
        statement = \
            '\t'*indent + 'MERGE (y :mobile_number{\n' + \
            '\t'*indent + '\tentity_type: "' + x[header.index('entity_type')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tbundle: "' + x[header.index('bundle')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tdeleted: toInteger("' + x[header.index('deleted')] + '"),\n' + \
            '\t'*indent + '\tentity_id: toInteger("' + x[header.index('entity_id')] + '"),\n' + \
            '\t'*indent + '\trevision_id: toInteger("' + x[header.index('revision_id')] + '"),\n' + \
            '\t'*indent + '\tlanguage: "' + x[header.index('language')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tdelta: toInteger("' + x[header.index('delta')] + '"),\n' + \
            '\t'*indent + '\tvalue: "' + x[header.index(fieldname + '_value')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tcountry: "' + x[header.index(fieldname + '_country')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tlocal_number: "' + x[header.index(fieldname + '_local_number')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tverified: toInteger("' + x[header.index(fieldname+'_verified')] + '"),\n' + \
            '\t'*indent + '\ttfa: toInteger("' + x[header.index(fieldname+'_tfa')] + '")\n' + \
            '\t'*indent + '})'
        return statement

    def field_geofield_structure(self, header, x, fieldname, indent=0):
        statement = \
            '\t'*indent + 'MERGE (y :geofield{\n' + \
            '\t'*indent + '\tentity_type: "' + x[header.index('entity_type')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tbundle: "' + x[header.index('bundle')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tdeleted: toInteger("' + x[header.index('deleted')] + '"),\n' + \
            '\t'*indent + '\tentity_id: toInteger("' + x[header.index('entity_id')] + '"),\n' + \
            '\t'*indent + '\trevision_id: toInteger("' + x[header.index('revision_id')] + '"),\n' + \
            '\t'*indent + '\tlanguage: "' + x[header.index('language')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tdelta: toInteger("' + x[header.index('delta')] + '"),\n' + \
            '\t'*indent + '\tgeom: "' + x[header.index(fieldname + '_geom')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tgeo_type: "' + x[header.index(fieldname + '_geo_type')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tlat: toFloat("' + x[header.index(fieldname+'_lat')] + '"),\n' + \
            '\t'*indent + '\tlon: toFloat("' + x[header.index(fieldname+'_lon')] + '"),\n' + \
            '\t'*indent + '\tleft: toFloat("' + x[header.index(fieldname+'_left')] + '"),\n' + \
            '\t'*indent + '\tright: toFloat("' + x[header.index(fieldname+'_right')] + '"),\n' + \
            '\t'*indent + '\ttop: toFloat("' + x[header.index(fieldname+'_top')] + '"),\n' + \
            '\t'*indent + '\tbottom: toFloat("' + x[header.index(fieldname+'_bottom')] + '"),\n' + \
            '\t'*indent + '\tgeohash: "' + x[header.index(fieldname + '_geohash')].replace('"', '\\"') + '"\n' + \
            '\t'*indent + '})'
        return statement

    def field_number_integer_structure(self, header, x, fieldname, indent=0):
        statement = \
            '\t'*indent + 'MERGE (y :number_integer{\n' + \
            '\t'*indent + '\tentity_type: "' + x[header.index('entity_type')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tbundle: "' + x[header.index('bundle')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tdeleted: toInteger("' + x[header.index('deleted')] + '"),\n' + \
            '\t'*indent + '\tentity_id: toInteger("' + x[header.index('entity_id')] + '"),\n' + \
            '\t'*indent + '\trevision_id: toInteger("' + x[header.index('revision_id')] + '"),\n' + \
            '\t'*indent + '\tlanguage: "' + x[header.index('language')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tdelta: toInteger("' + x[header.index('delta')] + '"),\n' + \
            '\t'*indent + '\tvalue: toInteger("' + x[header.index(fieldname+'_value')] + '")\n' + \
            '\t'*indent + '})'
        return statement

    def field_link_field_structure(self, header, x, fieldname, indent=0):
        statement = \
            '\t'*indent + 'MERGE (y :link_field{\n' + \
            '\t'*indent + '\tentity_type: "' + x[header.index('entity_type')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tbundle: "' + x[header.index('bundle')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tdeleted: toInteger("' + x[header.index('deleted')] + '"),\n' + \
            '\t'*indent + '\tentity_id: toInteger("' + x[header.index('entity_id')] + '"),\n' + \
            '\t'*indent + '\trevision_id: toInteger("' + x[header.index('revision_id')] + '"),\n' + \
            '\t'*indent + '\tlanguage: "' + x[header.index('language')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tdelta: toInteger("' + x[header.index('delta')] + '"),\n' + \
            '\t'*indent + '\turl: "' + x[header.index(fieldname + '_url')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\ttitle: "' + x[header.index(fieldname + '_title')].replace('"', '\\"') + '",\n' + \
            '\t'*indent + '\tattributes: "' + x[header.index(fieldname + '_attributes')].replace('"', '\\"') + '"\n' + \
            '\t'*indent + '})'
        return statement
    