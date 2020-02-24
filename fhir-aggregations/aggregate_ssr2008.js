/*
 * Merge fix and acte tables
 * 
 * Execute aggregation:
 * mongo --authenticationDatabase "bdname" --username "username" -p "pwd" aggregate_add_fields-ssr.js
 */

db = db.getSiblingDB('bdname')

db.result.drop()

db.ssr2008fix.aggregate([
	{$lookup: {from:"ssr2008acte", localField:"idnum", foreignField:"idnum", as:"actes"}},
	{$project: {"actes._id": false}},
	{$out: "result"}
])

print(db.result.find().count())

db.result.createIndex({idnum:1})

db.result.renameCollection("ssr2008_aggregate")
