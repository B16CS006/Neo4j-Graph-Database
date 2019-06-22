from neo4j import GraphDatabase

class DatabaseHandler(object):
    def __init__(self, uri, user, password):
        self.database_dir = 'file:///home/krrishna/Workspace/NPBridge/neo4j/ngov2/'
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self._driver.close()

    def load_nodes(self):
        try:
            with self._driver.session() as session:
                session.write_transaction(lambda tx: tx.run(
                    "USING PERIODIC COMMIT 500 "
                    "LOAD CSV WITH HEADERS FROM 'file:///home/krrishna/Workspace/NPBridge/neo4j/ngov2/node.csv' AS x "
                    "MERGE (node:node{ "
                        "nid: toInteger(x.nid), "
                        "vid: toInteger(x.vid), "
                        "type: x.type, "
                        "title: x.title, "
                        "language: x.language, "
                        "uid: toInteger(x.uid), "
                        "status: toInteger(x.status), "
                        "created: toInteger(x.created), "
                        "changed: toInteger(x.changed), "
                        "comment: toInteger(x.comment), "
                        "promote: toInteger(x.promote), "
                        "sticky: toInteger(x.sticky), "
                        "tnid: toInteger(x.tnid), "
                        "translate: toInteger(x.translate) "
                    "}) "
                    "return x "
                ))
            print('Nodes Successful Loaded')
            return True
        except Exception as e:
            print(e)
        return False

    def load_fields_collection_item(self):
        try:
            with self._driver.session() as session:
                session.write_transaction(lambda tx: tx.run(
                    "USING PERIODIC COMMIT 500 "
                    "LOAD CSV WITH HEADERS FROM 'file:///home/krrishna/Workspace/NPBridge/neo4j/ngov2/field_collection_item.csv' AS x "
                    "MERGE (node:field_collection_item{ "
                        "item_id: toInteger(x.item_id), "
                        "revision_id: toInteger(x.revision_id), "
                        "field_name: x.field_name, "
                        "archived: toInteger(x.archived) "
                    "}) "
                    "return node "
                ))
            print('Field Collection Item Successful Loaded')
            return True
        except Exception as e:
            print(e)
        return False

    def load_taxonomy_term(self):
        try:
            with self._driver.session() as session:
                session.write_transaction(lambda tx: tx.run(
                    "USING PERIODIC COMMIT 500\n"
                    "LOAD CSV WITH HEADERS FROM 'file:///home/krrishna/Workspace/NPBridge/neo4j/ngov2/taxonomy_term_data.csv' AS x "
                    "MERGE (term:taxonomy_term{ "
                    "    tid: toInteger(x.tid), "
                    "    vid: toInteger(x.vid), "
                    "    name: x.name, "
                    "    description: coalesce(x.description,''), "
                    "    format: coalesce(x.format, ''), "
                    "    weight: toInteger(x.weight) "
                    "}) "
                    "RETURN term "
                ))
            print('Taxonomy Term Successful Loaded')
            return True
        except Exception as e:
            print(e)
        return False

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

    def load_database(self):
        try:
            self.load_nodes()
            self.load_fields_collection_item()
            self.load_taxonomy_term()
            self.load_fields()
            print('Database succssfully Loaded')
            return True
        except Exception as e:
            print(e)
        return False

    def delete_whole_database(self):
        a = input('Are you sure about it(y/N): ')
        if(a != 'y' and a != 'Y'):
            print('Cancled')
            return False
        try:
            with self._driver.session() as session:
                session.write_transaction(lambda tx: tx.run(
                    "MATCH (n) DETACH DELETE n"
                ))
            print('Whole Database Successfully Deleted')
            return True
        except Exception as e:
            print(e)
        return False
