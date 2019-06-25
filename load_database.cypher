//****************************************************************************************************************************************************//
//////////////////////////////////////////////////////////////////DATABASE is STARTED BUILDING//////////////////////////////////////////////////////////
//****************************************************************************************************************************************************//

// Load node with title
USING PERIODIC COMMIET 500
LOAD CSV WITH HEADERS FROM 'file:///home/krrishna/Workspace/NPBridge/drupal/node.csv' AS x
MERGE (node:node{
    nid: toInteger(x.nid),
    vid: toInteger(x.vid),
    type: x.type,
    title: x.title,
    language: x.language,
    uid: toInteger(x.uid),
    status: toInteger(x.status),
    created: toInteger(x.created),
    changed: toInteger(x.changed),
    comment: toInteger(x.comment),
    promote: toInteger(x.promote),
    sticky: toInteger(x.sticky),
    tnid: toInteger(x.tnid),
    translate: toInteger(x.translate)
})
return node
// MERGE (title:title{
//     title: x.title
// })
// MERGE (node)-[:TITLE]->(title)


// Load Taxonomy term
USING PERIODIC COMMIET 500
LOAD CSV WITH HEADERS FROM 'file:///home/krrishna/Workspace/NPBridge/drupal/taxonomy_term_data.csv' AS x
MERGE (term:taxonomy_term{
    tid: toInteger(x.tid),
    vid: toInteger(x.vid),
    name: x.name,
    description: coalesce(x.description,''),
    format: coalesce(x.format, ''),
    weight: toInteger(x.weight)
})
return term

// Load Field collection item
USING PERIODIC COMMIET 500
LOAD CSV WITH HEADERS FROM 'file:///home/krrishna/Workspace/NPBridge/drupal/field_collection_item.csv' AS x
MERGE (node:field_collection_item{
    item_id: toInteger(x.item_id),
    revision_id: toInteger(x.revision_id),
    field_name: x.field_name,
    archived: toInteger(x.archived)
})
return node


//Load fields and make relationship also
USING PERIODIC COMMIET 500
LOAD CSV WITH HEADERS FROM 'file:///home/krrishna/Workspace/NPBridge/drupal/field_config.csv' AS x
WITH x.field_name as field_names, x.type as field_types
UNWIND field_names as field_name
LOAD CSV WITH HEADERS FROM 'file:///home/krrishna/Workspace/NPBridge/drupal/field_data_'+field_name+'.csv' AS x
match (n) where x.entity_type in labels(n) and
case
	when exists(n.nid) then toInteger(n.nid)
    when exists(n.item_id) then toInteger(n.item_id)
    when exists(n.tid) then toInteger(n.tid)
    else -1
end = toInteger(x.entity_id)

CALL apoc.do.case([
    field_types = 'entityreference', "MATCH (y:node) WHERE toInteger(y.nid) = toInteger(x[field_name+'_target_id']) RETURN y",
    field_types = 'taxonomy_term_reference', "MATCH (y:taxonomy_term) WHERE toInteger(y.tid) = toInteger(x[field_name+'_tid']) RETURN y",
    field_types = 'field_collection', "MATCH (y:field_collection_item) WHERE toInteger(y.item_id) = toInteger(x[field_name+'_value']) RETURN y",
    field_types = 'text_with_summary', "MERGE(y:text_with_summary{ entity_id: toInteger(x.entity_id), entity_type: x.entity_type, bundle: x.bundle, deleted: toInteger(x.deleted), language: x.language, revision_id: toInteger(x.revision_id), delta: toInteger(x.delta), value: coalesce(x[field_name+'_value'], ''), summary: coalesce(x[field_name+'_summary'], ''), format: coalesce(x[field_name+'_format'], '') }) RETURN y",
    field_types = 'text_long', "MERGE(y:text_long{ entity_id: toInteger(x.entity_id), entity_type: x.entity_type, bundle: x.bundle, deleted: toInteger(x.deleted), language: x.language, revision_id: toInteger(x.revision_id), delta: toInteger(x.delta), value: coalesce(x[field_name+'_value'], ''), format: coalesce(x[field_name+'_format'], '') }) RETURN y",
    field_types = 'image', "MERGE(y:image{ entity_id: toInteger(x.entity_id), entity_type: x.entity_type, bundle: x.bundle, deleted: toInteger(x.deleted), language: x.language, revision_id: toInteger(x.revision_id), delta: toInteger(x.delta), title: coalesce(x[field_name+'_title'], ''), alt: coalesce(x[field_name+'_alt'], ''), fid: toFloat(x[field_name+'_fid']), width: toFloat(x[field_name+'_width']), height: toFloat(x[field_name+'_height']) }) RETURN y",
    field_types = 'list_text', "MERGE(y:list_text{ entity_id: toInteger(x.entity_id), entity_type: x.entity_type, bundle: x.bundle, deleted: toInteger(x.deleted), language: x.language, revision_id: toInteger(x.revision_id), delta: toInteger(x.delta), value: coalesce(x[field_name+'_value'], '') }) RETURN y",
    field_types = 'text', "MERGE(y:text{ entity_id: toInteger(x.entity_id), entity_type: x.entity_type, bundle: x.bundle, deleted: toInteger(x.deleted), language: x.language, revision_id: toInteger(x.revision_id), delta: toInteger(x.delta), value: coalesce(x[field_name+'_value'], ''), format: coalesce(x[field_name+'_format'], '') }) RETURN y",
    field_types = 'mobile_number', "MERGE(y:mobile_number{ entity_id: toInteger(x.entity_id), entity_type: x.entity_type, bundle: x.bundle, deleted: toInteger(x.deleted), language: x.language, revision_id: toInteger(x.revision_id), delta: toInteger(x.delta), value: coalesce(x[field_name+'_value'], ''), country: coalesce(x[field_name+'_country'], ''), local_number: coalesce(x[field_name+'_local_number'], ''), verified: toInteger(x[field_name+'_verified']), tfa: toInteger(x[field_name+'_tfa']) }) RETURN y",
    field_types = 'email', "MERGE(y:email{ entity_id: toInteger(x.entity_id), entity_type: x.entity_type, bundle: x.bundle, deleted: toInteger(x.deleted), language: x.language, revision_id: toInteger(x.revision_id), delta: toInteger(x.delta), email: coalesce(x[field_name+'_email'], '') }) RETURN y",
    field_types = 'geofield', "MERGE(y:geofield{ entity_id: toInteger(x.entity_id), entity_type: x.entity_type, bundle: x.bundle, deleted: toInteger(x.deleted), language: x.language, revision_id: toInteger(x.revision_id), delta: toInteger(x.delta), geom: coalesce(x[field_name+'_geom'], ''), geo_type: coalesce(x[field_name+'_geo_type'], ''), lat: toFloat(x[field_name+'_lat']), lon: toFloat(x[field_name+'_lon']), left: toFloat(x[field_name+'_left']), right: toFloat(x[field_name+'_right']), top: toFloat(x[field_name+'_top']), bottom: toFloat(x[field_name+'_bottom']), geohash: coalesce(x[field_name+'_geohash'], '') }) RETURN y",
    field_types = 'number_integer', "MERGE(y:number_integer{ entity_id: toInteger(x.entity_id), entity_type: x.entity_type, bundle: x.bundle, deleted: toInteger(x.deleted), language: x.language, revision_id: toInteger(x.revision_id), delta: toInteger(x.delta), value: toInteger(x[field_name+'_value']) }) RETURN y",
    field_types = 'link_field', "MERGE(y:link_field{ entity_id: toInteger(x.entity_id), entity_type: x.entity_type, bundle: x.bundle, deleted: toInteger(x.deleted), language: x.language, revision_id: toInteger(x.revision_id), delta: toInteger(x.delta), url: coalesce(x[field_name+'_url'], ''), title: coalesce(x[field_name+'_title'], ''), attributes: coalesce(x[field_name+'_attributes'], '') }) RETURN y"
    ], "RETURN NULL",{field_name:field_name, x:x}
) YIELD value
WITH field_name, n, value.y AS y WHERE y IS NOT NULL

CALL apoc.merge.relationship.eager(n, field_name, {},{},y,{}) YIELD rel as result
return n,y

//****************************************************************************************************************************************************//
/////////////////////////////////////////////////////////////DATABASE id COMPLETED//////////////////////////////////////////////////////////////////////
//****************************************************************************************************************************************************//

//////////////////////////////////////// Load each field_collection_item and make a relationship with entity //////////
LOAD CSV WITH HEADERS FROM "file:///home/krrishna/Workspace/NPBridge/drupal/field_collection_item.csv" AS x 
WITH distinct x.field_name as field_names
UNWIND field_names AS field_name

LOAD CSV WITH HEADERS FROM "file:///home/krrishna/Workspace/NPBridge/drupal/field_collection_item.csv" AS x 
UNWIND x.field_name AS field_name
WITH field_name
LOAD CSV WITH HEADERS FROM "file:///home/krrishna/Workspace/NPBridge/drupal/field_data_"+field_name+".csv" AS x
MATCH (n)
WHERE x.entity_type in labels(n) AND
CASE
	WHEN exists(n.nid) THEN toInteger(n.nid)
    WHEN exists(n.item_id) THEN toInteger(n.item_id)
    WHEN exists(n.tid) THEN toInteger(n.tid)
	ELSE -1
END = toInteger(x.entity_id)
WITH n,x
MATCH (y:field_collection_item) WHERE toInteger(y.item_id) = toInteger(x.field_employee_details_value)
MERGE (n)-[:FIELD_EMPLOYEE_DETAILS]->(y)
RETURN *



////////////////////////////////////////////////////////////////////// Load Fields////////////////////////////////////////////////////
LOAD CSV WITH HEADERS FROM "file:///home/krrishna/Workspace/NPBridge/drupal/field_config.csv" AS x
WITH x.field_name as field_names, x.type as field_types
UNWIND field_names as field_name
LOAD CSV WITH HEADERS FROM "file:///home/krrishna/Workspace/NPBridge/drupal/field_data_"+field_name+".csv" AS x
WITH x, CASE field_types
    WHEN 'email' THEN {
        label: 'email',
        email: coalesce(x[field_name+'_email'], '')
    }
    WHEN 'number_integer' THEN {
        label: 'number_integer',
        value: toInteger(x[field_name+'_value'])
    }
    WHEN 'list_text' THEN {
        label: 'list_text',
        value: coalesce(x[field_name+'_value'], '')
    }
    WHEN 'text_long' THEN {
        label: 'text_long',
        value: coalesce(x[field_name+'_value'], ''),
        format: coalesce(x[field_name+'_format'], '')
    }
    WHEN 'text' THEN {
        label: 'text',
        value: coalesce(x[field_name+'_value'], ''),
        format: coalesce(x[field_name+'_format'], '')
    }
    WHEN 'link_field' THEN {
        label: 'link_field',
        url: coalesce(x[field_name+'_url'], ''),
        title: coalesce(x[field_name+'_title'], ''),
        attributes: coalesce(x[field_name+'_attributes'], '')
    }
    WHEN 'text_with_summary' THEN {
        label: 'text_with_summary',
        value: coalesce(x[field_name+'_value'], ''),
        summary: coalesce(x[field_name+'_summary'], ''),
        format: coalesce(x[field_name+'_format'], '')
    }
    WHEN 'image' THEN {
        label: 'image',
        title: coalesce(x[field_name+'_title'], ''),
        alt: coalesce(x[field_name+'_alt'], ''),
        fid: toFloat(x[field_name+'_fid']),
        width: toFloat(x[field_name+'_width']),
        height: toFloat(x[field_name+'_height'])
    }
    WHEN 'mobile_number' THEN {
        label: 'mobile_number',
        value: coalesce(x[field_name+'_value'], ''),
        country: coalesce(x[field_name+'_country'], ''),
        local_number: coalesce(x[field_name+'_local_number'], ''),
        verified: toInteger(x[field_name+'_verified']),
        tfa: toInteger(x[field_name+'_tfa'])
    }
    WHEN 'geofield' THEN {
        label: 'geofield',
        geom: coalesce(x[field_name+'_geom'], ''),
        geo_type: coalesce(x[field_name+'_geo_type'], ''),
        lat: toFloat(x[field_name+'_lat']),
        lon: toFloat(x[field_name+'_lon']),
        left: toFloat(x[field_name+'_left']),
        right: toFloat(x[field_name+'_right']),
        top: toFloat(x[field_name+'_top']),
        bottom: toFloat(x[field_name+'_bottom']),
        geohash: coalesce(x[field_name+'_geohash'], '')
    }
END AS map where map is not null
CREATE(y{
    entity_id: toInteger(x.entity_id),
    entity_type: x.entity_type,
    bundle: x.bundle,
    deleted: toInteger(x.deleted),
    language: x.language,
    revision_id: toInteger(x.revision_id),
    delta: toInteger(x.delta)
})
SET y += map
RETURN y



////////////////////////////////////////////////////////////////////////LOAD FIELD WITH RELATIONSHIP USING APOC/////////////////////////////////////////////////
LOAD CSV WITH HEADERS FROM "file:///home/krrishna/Workspace/NPBridge/drupal/field_config.csv" AS x
WITH x.field_name as field_names, x.type as field_types
UNWIND field_names as field_name
LOAD CSV WITH HEADERS FROM "file:///home/krrishna/Workspace/NPBridge/drupal/field_data_"+field_name+".csv" AS x
match (n) where x.entity_type in labels(n) and
case
	when exists(n.nid) then toInteger(n.nid)
    when exists(n.item_id) then toInteger(n.item_id)
    when exists(n.tid) then toInteger(n.tid)
    else -1
end = toInteger(x.entity_id)

CALL apoc.do.case([
    field_types = 'entityreference', "CALL apoc.merge.node(['node'], {nid: toInteger(x[field_name+'_target_id'])},{},{}) YIELD node RETURN node",
    field_types = 'taxonomy_term_reference', "CALL apoc.merge.node(['taxonomy_term'], {tid: toInteger(x[field_name+'_tid'])},{},{}) YIELD node RETURN node",
    field_types = 'field_collection', "CALL apoc.merge.node(['field_collection_item'], {item_id: toInteger(x[field_name+'_value'])},{},{}) YIELD node RETURN node",
    field_types = 'text_with_summary', "CALL apoc.merge.node(['text_with_summary'], { entity_id: toInteger(x.entity_id), entity_type: x.entity_type, bundle: x.bundle, deleted: toInteger(x.deleted), language: x.language, revision_id: toInteger(x.revision_id), delta: toInteger(x.delta) }, { value: coalesce(x[field_name+'_value'], ''), summary: coalesce(x[field_name+'_summary'], ''), format: coalesce(x[field_name+'_format'], '') },{}) YIELD node RETURN node",
    field_types = 'text_long', "CALL apoc.merge.node(['text_long'], { entity_id: toInteger(x.entity_id), entity_type: x.entity_type, bundle: x.bundle, deleted: toInteger(x.deleted), language: x.language, revision_id: toInteger(x.revision_id), delta: toInteger(x.delta) }, { value: coalesce(x[field_name+'_value'], ''), format: coalesce(x[field_name+'_format'], '') },{}) YIELD node RETURN node",
    field_types = 'image', "CALL apoc.merge.node(['image'], { entity_id: toInteger(x.entity_id), entity_type: x.entity_type, bundle: x.bundle, deleted: toInteger(x.deleted), language: x.language, revision_id: toInteger(x.revision_id), delta: toInteger(x.delta) }, { title: coalesce(x[field_name+'_title'], ''), alt: coalesce(x[field_name+'_alt'], ''), fid: toFloat(x[field_name+'_fid']), width: toFloat(x[field_name+'_width']), height: toFloat(x[field_name+'_height']) },{}) YIELD node RETURN node",
    field_types = 'list_text', "CALL apoc.merge.node(['list_text'], { entity_id: toInteger(x.entity_id), entity_type: x.entity_type, bundle: x.bundle, deleted: toInteger(x.deleted), language: x.language, revision_id: toInteger(x.revision_id), delta: toInteger(x.delta) }, { value: coalesce(x[field_name+'_value'], '') },{}) YIELD node RETURN node",
    field_types = 'text', "CALL apoc.merge.node(['text'], { entity_id: toInteger(x.entity_id), entity_type: x.entity_type, bundle: x.bundle, deleted: toInteger(x.deleted), language: x.language, revision_id: toInteger(x.revision_id), delta: toInteger(x.delta) }, { value: coalesce(x[field_name+'_value'], ''), format: coalesce(x[field_name+'_format'], '') },{}) YIELD node RETURN node",
    field_types = 'mobile_number', "CALL apoc.merge.node(['mobile_number'], { entity_id: toInteger(x.entity_id), entity_type: x.entity_type, bundle: x.bundle, deleted: toInteger(x.deleted), language: x.language, revision_id: toInteger(x.revision_id), delta: toInteger(x.delta) }, { value: coalesce(x[field_name+'_value'], ''), country: coalesce(x[field_name+'_country'], ''), local_number: coalesce(x[field_name+'_local_number'], ''), verified: toInteger(x[field_name+'_verified']), tfa: toInteger(x[field_name+'_tfa']) },{}) YIELD node RETURN node",
    field_types = 'email', "CALL apoc.merge.node(['email'], { entity_id: toInteger(x.entity_id), entity_type: x.entity_type, bundle: x.bundle, deleted: toInteger(x.deleted), language: x.language, revision_id: toInteger(x.revision_id), delta: toInteger(x.delta) }, { email: coalesce(x[field_name+'_email'], '') },{}) YIELD node RETURN node",
    field_types = 'geofield', "CALL apoc.merge.node(['geofield'], { entity_id: toInteger(x.entity_id), entity_type: x.entity_type, bundle: x.bundle, deleted: toInteger(x.deleted), language: x.language, revision_id: toInteger(x.revision_id), delta: toInteger(x.delta) }, { geom: coalesce(x[field_name+'_geom'], ''), geo_type: coalesce(x[field_name+'_geo_type'], ''), lat: toFloat(x[field_name+'_lat']), lon: toFloat(x[field_name+'_lon']), left: toFloat(x[field_name+'_left']), right: toFloat(x[field_name+'_right']), top: toFloat(x[field_name+'_top']), bottom: toFloat(x[field_name+'_bottom']), geohash: coalesce(x[field_name+'_geohash'], '') },{}) YIELD node RETURN node",
    field_types = 'number_integer', "CALL apoc.merge.node(['number_integer'], { entity_id: toInteger(x.entity_id), entity_type: x.entity_type, bundle: x.bundle, deleted: toInteger(x.deleted), language: x.language, revision_id: toInteger(x.revision_id), delta: toInteger(x.delta) }, { value: toInteger(x[field_name+'_value']) },{}) YIELD node RETURN node",
    field_types = 'link_field', "CALL apoc.merge.node(['link_field'], { entity_id: toInteger(x.entity_id), entity_type: x.entity_type, bundle: x.bundle, deleted: toInteger(x.deleted), language: x.language, revision_id: toInteger(x.revision_id), delta: toInteger(x.delta) }, { url: coalesce(x[field_name+'_url'], ''), title: coalesce(x[field_name+'_title'], ''), attributes: coalesce(x[field_name+'_attributes'], '') },{}) YIELD node RETURN node"
    ], "RETURN NULL",{field_name:field_name, x:x}
) YIELD value
WITH field_name, n, value.node AS y WHERE y IS NOT NULL

CALL apoc.merge.relationship.eager(n, field_name, {},{},y,{}) YIELD rel as result
return n,y


//****************************************************************************************************************************************************//
/////////////////////////////////////////////////////////////Some Special Query//////////////////////////////////////////////////////////////////////
//****************************************************************************************************************************************************//
MATCH (n:field_collection_item) with [n] AS nodes unwind nodes AS node RETURN node
FOREACH (ignoreMe in CASE WHEN condition THEN [1] ELSE [] END | )

