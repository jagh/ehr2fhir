/*  
 * Filter to compose the CDA only for the first week
 * 
 * Execute aggregation:
 * mongo --authenticationDatabase "bdname" --username "username" -p "pwd" aggregate_add_fields-ssr.js
 */


db = db.getSiblingDB('bdname')

db.result.drop()

db.ssr2008_aggregate_entity_group.aggregate([

	{$match: { "x3_hospitalization_details.sequence_number": 1 }},
    {$out: "result"}
])


print(db.result.find().count())

db.result.createIndex({Idnum:1})

db.result.renameCollection("ssr2008_aggregate_entity_group_week1")


