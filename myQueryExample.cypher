//Using APOC

//apoc help functions
CALL apoc.help("addnode")

//Create node
CALL apoc.create.node(['City','India'], {name:'Bikaner'}) Yield node
RETURN node

//Merge node
CALL apoc.merge.node(['Country'],{name:'JAPAN'},{},{}) Yield node
RETURN node

//Case
CALL apoc.do.case([
    true    "MATCH (n:Country{name:'China'}) return n",
	false,  "CREATE (n:Country{name:'China'}) return n", 
	false   "CREATE (:Country{name:'Japan'})",
	false   "CALL apoc.merge.node(['Country'],{name:'JAPAN'},{},{})",
	false   "CALL apoc.merge.node(['Country'],{name:'Bharat'},{},{})"
    ],'return false',{}
) YIELD value AS result
RETURN result


// Dynamic Relationship
WITH 'IN_SAME_STATE' AS relation
MATCH (n:City:India{name:'Bikaner'}), (m:City:India{name:'Jaipur'})
CALL apoc.create.relationship(n,relation,{apart:'100km'}, m) Yield rel as result
RETURN result


//Set Relationship Properties
WITH 20 as relationship_id
CALL apoc.create.setRelProperties(relationship_id, ["has_war",'some'], ['no',null]) Yield rel AS result
RETURN result

// Set Labels
MATCH (n{name:'Bikaner'}), (m{name:'Jaipur'})
CALL apoc.create.setLabels([n,m],['City','India']) Yield node AS result
RETURN result

