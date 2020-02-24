/*  
 * Bucketization for the rehabilitation time
 * 
 * Execute aggregation:
 * mongo --authenticationDatabase "bdname" --username "username" -p "pwd" aggregate_add_fields-ssr.js
 */


db = db.getSiblingDB('bdname')

db.result.drop()

db.ssr2008_aggregate.aggregate([

    //{$limit: 1000},
    //{$match: {"Idnum":"010002129000021401"}},

	{$addFields: {"age_group": {
		$switch: {
			branches: [
				{ case: { $lt: ["$age",6] }, then: "G1" },
				{ case: { $and: [ {$gte:["$age",6]}, {$lt:["$age", 12]} ]}, then: "G2" },
				{ case: { $and: [ {$gte:["$age",12]}, {$lt:["$age", 17]} ]}, then: "G3" },
				{ case: { $and: [ {$gte:["$age",17]}, {$lt:["$age", 29]} ]}, then: "G4" },
				{ case: { $and: [ {$gte:["$age",29]}, {$lt:["$age", 50]} ]}, then: "G5" },
				{ case: { $and: [ {$gte:["$age",50]}, {$lt:["$age", 74]} ]}, then: "G6" },
				{ case: { $gte: ["$age",74] }, then: "G7" }
			],
			default: "N.A"
 		}
	}}},



	{$addFields: {"numdays_hospitalized": {
		$sum: ["$Nbjour_sem", "$Nbjour_we"],
	}}},



	{$addFields: {"date_chir_group": {
		$switch: {
			branches: [
				{ case: { $lt: ["$Delai_date_chir",1] }, then: "G1" },
				{ case: { $and: [ {$gte:["$Delai_date_chir",1]}, {$lt:["$Delai_date_chir", 3]} ]}, then: "G2" },
				{ case: { $and: [ {$gte:["$Delai_date_chir",3]}, {$lt:["$Delai_date_chir", 7]} ]}, then: "G3" },
				{ case: { $and: [ {$gte:["$Delai_date_chir",7]}, {$lt:["$Delai_date_chir", 11]} ]}, then: "G4" },
				{ case: { $and: [ {$gte:["$Delai_date_chir",11]}, {$lt:["$Delai_date_chir", 14]} ]}, then: "G5" },
				{ case: { $and: [ {$gte:["$Delai_date_chir",14]}, {$lt:["$Delai_date_chir", 46]} ]}, then: "G6" },
				{ case: { $and: [ {$gte:["$Delai_date_chir",46]}, {$lt:["$Delai_date_chir", 69]} ]}, then: "G7" },
				{ case: { $and: [ {$gte:["$Delai_date_chir",69]}, {$lt:["$Delai_date_chir", 146]} ]}, then: "G8" },
				{ case: { $and: [ {$gte:["$Delai_date_chir",146]}, {$lt:["$Delai_date_chir", 366]} ]}, then: "G9" },
				{ case: { $gte: ["Delai_date_chir", 366]}, then: "G10"}
			],
			default: "N.A"
		}
	}}},



	{$addFields: {"Reeduc_meca_group": {
		$switch: {
			branches: [
				{ case: { $lt: ["$Reeduc_meca",1] }, then: "G1" },
				{ case: { $and: [ {$gte:["$Reeduc_meca",1]}, {$lt:["$Reeduc_meca", 30]} ]}, then: "G2" },
				{ case: { $and: [ {$gte:["$Reeduc_meca",30]}, {$lt:["$Reeduc_meca", 120]} ]}, then: "G3" },
				{ case: { $and: [ {$gte:["$Reeduc_meca",120]}, {$lt:["$Reeduc_meca", 175]} ]}, then: "G4" },
				{ case: { $and: [ {$gte:["$Reeduc_meca",175]}, {$lt:["$Reeduc_meca", 230]} ]}, then: "G5" },
				{ case: { $and: [ {$gte:["$Reeduc_meca",230]}, {$lt:["$Reeduc_meca", 400]} ]}, then: "G6" },
				{ case: { $gte: ["$Reeduc_meca",400] }, then: "G7" }
			],
			default: "N.A"
		}
	}}},



    {$addFields: {"Reeduc_sensoriM_group": {
        $switch: {
            branches: [
                { case: { $lt: ["$Reeduc_sensoriM",1] }, then: "G1" },
                { case: { $and: [ {$gte:["$Reeduc_sensoriM",1]}, {$lt:["$Reeduc_sensoriM", 50]} ]}, then: "G2" },
                { case: { $and: [ {$gte:["$Reeduc_sensoriM",50]}, {$lt:["$Reeduc_sensoriM", 90]} ]}, then: "G3" },
                { case: { $and: [ {$gte:["$Reeduc_sensoriM",90]}, {$lt:["$Reeduc_sensoriM", 225]} ]}, then: "G4" },
                { case: { $gte: ["$Reeduc_sensoriM",225] }, then: "G5" }
            ],
            default: "N.A"
        }
    }}},




	{$addFields: {"Reeduc_neuropsy_group": {
		$switch: {
			branches: [
				{ case: { $lt: ["$Reeduc_neuropsy",1] }, then: "G1" },
				{ case: { $and: [ {$gte:["$Reeduc_neuropsy",1]}, {$lt:["$Reeduc_neuropsy", 30]} ]}, then: "G2" },
				{ case: { $and: [ {$gte:["$Reeduc_neuropsy",30]}, {$lt:["$Reeduc_neuropsy", 60]} ]}, then: "G3" },
				{ case: { $and: [ {$gte:["$Reeduc_neuropsy",60]}, {$lt:["$Reeduc_neuropsy", 185]} ]}, then: "G4" },
				{ case: { $gte: ["$Reeduc_neuropsy",185] }, then: "G5" }
			],
			default: "N.A"
		}
	}}},



	{$addFields: {"Reeduc_cardioresp_group": {
		$switch: {
			branches: [
				{ case: { $lt: ["$Reeduc_cardioresp",1] }, then: "G1" },
				{ case: { $and: [ {$gte:["$Reeduc_cardioresp",1]}, {$lt:["$Reeduc_cardioresp", 15]} ]}, then: "G2" },
				{ case: { $and: [ {$gte:["$Reeduc_cardioresp",15]}, {$lt:["$Reeduc_cardioresp", 75]} ]}, then: "G3" },
				{ case: { $and: [ {$gte:["$Reeduc_cardioresp",75]}, {$lt:["$Reeduc_cardioresp", 160]} ]}, then: "G4" },
				{ case: { $gte: ["$Reeduc_cardioresp",160] }, then: "G5" }
			],
			default: "N.A"
		}
	}}},




	{$addFields: {"Reeduc_nutri_group": {
		$switch: {
			branches: [
				{ case: { $lt: ["$Reeduc_nutri",1] }, then: "G1" },
				{ case: { $and: [ {$gte:["$Reeduc_nutri",1]}, {$lt:["$Reeduc_nutri", 25]} ]}, then: "G2" },
				{ case: { $and: [ {$gte:["$Reeduc_nutri",25]}, {$lt:["$Reeduc_nutri", 60]} ]}, then: "G3" },
				{ case: { $and: [ {$gte:["$Reeduc_nutri",60]}, {$lt:["$Reeduc_nutri", 580]} ]}, then: "G4" },
				{ case: { $gte: ["$Reeduc_cardioresp",580] }, then: "G5" }
			],
			default: "N.A"
		}
	}}},



	{$addFields: {"Reeduc_urosphinc_group": {
		$switch: {
			branches: [
				{ case: { $lt: ["$Reeduc_urosphinc",1] }, then: "G1" },
				{ case: { $and: [ {$gte:["$Reeduc_urosphinc",1]}, {$lt:["$Reeduc_urosphinc", 450]} ]}, then: "G2" },
				{ case: { $and: [ {$gte:["$Reeduc_urosphinc",450]}, {$lt:["$Reeduc_urosphinc", 500]} ]}, then: "G3" },
				{ case: { $and: [ {$gte:["$Reeduc_urosphinc",500]}, {$lt:["$Reeduc_urosphinc", 560]} ]}, then: "G4" },
				{ case: { $gte: ["$Reeduc_urosphinc",560] }, then: "G5" }
			],
			default: "N.A"
		}
	}}},



	{$addFields: {"Readap_reins_group": {
		$switch: {
			branches: [
				{ case: { $lt: ["$Readap_reins",1] }, then: "G1" },
				{ case: { $and: [ {$gte:["$Readap_reins",1]}, {$lt:["$Readap_reins", 20]} ]}, then: "G2" },
				{ case: { $and: [ {$gte:["$Readap_reins",20]}, {$lt:["$Readap_reins", 80]} ]}, then: "G3" },
				{ case: { $and: [ {$gte:["$Readap_reins",80]}, {$lt:["$Readap_reins", 130]} ]}, then: "G4" },
				{ case: { $and: [ {$gte:["$Readap_reins",130]}, {$lt:["$Readap_reins", 450]} ]}, then: "G5" },
				{ case: { $gte: ["$Readap_reins",450] }, then: "G6" }
			],
			default: "N.A"
		}
	}}},



	{$addFields: {"Appareillage_group": {
		$switch: {
			branches: [
				{ case: { $lt: ["$Appareillage",1] }, then: "G1" },
				{ case: { $and: [ {$gte:["$Appareillage",1]}, {$lt:["$Appareillage", 50]} ]}, then: "G2" },
				{ case: { $and: [ {$gte:["$Appareillage",50]}, {$lt:["$Appareillage", 100]} ]}, then: "G3" },
				{ case: { $and: [ {$gte:["$Appareillage",100]}, {$lt:["$Appareillage", 200]} ]}, then: "G4" },
				{ case: { $and: [ {$gte:["$Appareillage",200]}, {$lt:["$Appareillage", 500]} ]}, then: "G5" },
				{ case: { $gte: ["$Appareillage",500] }, then: "G6" }
			],
			default: "N.A"
		}
	}}},



	{$addFields: {"Reeduc_collective_group": {
		$switch: {
			branches: [
				{ case: { $lt: ["$Reeduc_collective",1] }, then: "G1" },
				{ case: { $and: [ {$gte:["$Reeduc_collective",1]}, {$lt:["$Reeduc_collective", 50]} ]}, then: "G2" },
				{ case: { $and: [ {$gte:["$Reeduc_collective",50]}, {$lt:["$Reeduc_collective", 100]} ]}, then: "G3" },
				{ case: { $and: [ {$gte:["$Reeduc_collective",100]}, {$lt:["$Reeduc_collective", 200]} ]}, then: "G4" },
				{ case: { $and: [ {$gte:["$Reeduc_collective",200]}, {$lt:["$Reeduc_collective", 300]} ]}, then: "G5" },
				{ case: { $gte: ["$Reeduc_collective",300] }, then: "G6" }
			],
			default: "N.A"
		}
	}}},



	{$addFields: {"Bilans_group": {
		$switch: {
			branches: [
				{ case: { $lt: ["$Bilans",1] }, then: "G1" },
				{ case: { $and: [ {$gte:["$Bilans",1]}, {$lt:["$Bilans", 10]} ]}, then: "G2" },
				{ case: { $and: [ {$gte:["$Bilans",10]}, {$lt:["$Bilans", 30]} ]}, then: "G3" },
				{ case: { $and: [ {$gte:["$Bilans",30]}, {$lt:["$Bilans", 45]} ]}, then: "G4" },
				{ case: { $and: [ {$gte:["$Bilans",45]}, {$lt:["$Bilans", 175]} ]}, then: "G5" },
				{ case: { $gte: ["$Bilans",175] }, then: "G6" }
			],
			default: "N.A"
		}
	}}},



	{$addFields: {"PhysioT_group": {
		$switch: {
			branches: [
				{ case: { $lt: ["$PhysioT",1] }, then: "G1" },
				{ case: { $and: [ {$gte:["$PhysioT",1]}, {$lt:["$PhysioT", 10]} ]}, then: "G2" },
				{ case: { $and: [ {$gte:["$PhysioT",10]}, {$lt:["$PhysioT", 40]} ]}, then: "G3" },
				{ case: { $and: [ {$gte:["$PhysioT",40]}, {$lt:["$PhysioT", 60]} ]}, then: "G4" },
				{ case: { $and: [ {$gte:["$PhysioT",60]}, {$lt:["$PhysioT", 120]} ]}, then: "G5" },
				{ case: { $gte: ["$PhysioT",120] }, then: "G6" }
			],
			default: "N.A"
		}
	}}},



	{$addFields: {"BalneoT_group": {
		$switch: {
			branches: [
				{ case: { $lt: ["$BalneoT",1] }, then: "G1" },
				{ case: { $and: [ {$gte:["$BalneoT",1]}, {$lt:["$BalneoT", 10]} ]}, then: "G2" },
				{ case: { $and: [ {$gte:["$BalneoT",10]}, {$lt:["$BalneoT", 40]} ]}, then: "G3" },
				{ case: { $and: [ {$gte:["$BalneoT",40]}, {$lt:["$BalneoT", 60]} ]}, then: "G4" },
				{ case: { $and: [ {$gte:["$BalneoT",60]}, {$lt:["$BalneoT", 120]} ]}, then: "G5" },
				{ case: { $gte: ["$BalneoT",120] }, then: "G6" }
			],
			default: "N.A"
		}
	}}},



	{$out: "result"}
])

print(db.result.find().count())

db.result.createIndex({Idnum:1})

db.result.renameCollection("ssr2008_aggregate_addfield")
