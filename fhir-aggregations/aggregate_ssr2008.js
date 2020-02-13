db = db.getSiblingDB('pmsi-2020')

db.result.drop()

db.ssr2008fix.aggregate([

    //{$limit: 1000},
    //{$match: {"Idnum":"40780389000024600"}},

    {$lookup: {from:"ssr2008acte", localField:"idnum", foreignField:"idnum", as:"actes"}},

    {$project: {"actes._id": false}},

    {$out: "result"}
])

print(db.result.find().count())

db.result.createIndex({idnum:1})

db.result.renameCollection("ssr2008_aggregate")
