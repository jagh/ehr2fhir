/*  
 * Composition of the clinical document architecture
 * 
 * Execute aggregation:
 * mongo --authenticationDatabase "bdname" --username "username" -p "pwd" aggregate_add_fields-ssr.js
 */


db = db.getSiblingDB('bdname')

db.result.drop()

db.ssr2008_aggregate_addfield.aggregate([


	{$addFields: {"x0_header": {
					"ID_RSA": "$idnum",
					"hospital": "$finess",
					"patient": "$clef",
					"patient_Rol": "Inpatient",
					"rsa_V": "$version"
	}}},

	{$project: {
				"finess": false, 
				"clef": false,
				"version": false,
				"_id": false,
				"idnum": false
	}},



	{$addFields: {"x1_demographics": {
					"age": "$age",
			        "sexe": "$sexe",
					"age_group": "$age_group",
					"activity": "$type_activite",
					"postal_code": "$geograph",
    }}},

	{$project: {
				"age": false, 
				"sexe": false,
				"age_group": false,
				"type_activite": false,
				"geograph": false
	}},




	{$addFields: {"x2_admission_details": {
					"input_mode": "$mode_entree",
					"input_source": "$prov_entree",
					"previous_state": "$anteriorite",
					"first_week": "$semaine_debut",
					"month": "$Mois",
					"year": "$Annee"
	}}},

	{$project: {
				"mode_entree": false,
				"prov_entree": false,
				"anteriorite": false,
				"semaine_debut": false,
				"Mois": false,
				"Annee": false
	}},



	{$addFields: {"x3_hospitalization_details": {
					//"numdays_week": "$Nbjour_sem",
					//"numdays_weekend": "$Nbjour_we",
					"numdays_hospitalized": {$toUpper: "$numdays_hospitalized"},
					"sequence_number": "$Num_seq",
					"surgery_time": "$date_chir_group",
	}}},

    {$project: {
				"Nbjour_sem": false, 
				"Nbjour_we": false,
				"numdays_hospitalized": false,
				"Num_seq": false,
				"Delai_date_chir": false,
				"date_chir_group": false
	}},



	{$addFields: {"x4_physical_dependence": {
					"dressing": "$Dep_habillage",
					"displacement": "$Dep_deplacement",
					"feeding": "$Dep_alimentation",
					"continence": "$Dep_continence",
					"wheelchair": "$Fauteuil_roulant",
	}}},

    {$project: {
				"Dep_habillage": false, 
				"Dep_deplacement": false,
				"Dep_alimentation": false,
				"Dep_continence": false,
				"Fauteuil_roulant": false
	}},



	{$addFields: {"x5_cognitive_dependence": {
					"comportement": "$Dep_comportement",
					"communication": "$Dep_relation",
	}}},

	{$project: {
				"Dep_comportement": false,
				"Dep_relation": false,
	}},



	{$addFields: {"x6_rehabilitation_time": {
					"mechanical_rehab": "$Reeduc_meca_group",
					"motorsensory_rehab": "$Reeduc_sensoriM_group",
					"neuropsychological_rehab": "$Reeduc_neuropsy_group",
					"cardiorespiratory_rehab": "$Reeduc_cardioresp_group",
					"nutritional_rehab": "$Reeduc_nutri_group",
					"urogenitalsphincter_rehab": "$Reeduc_urosphinc_group",
					"kidneys_rehab": "$Readap_reins_group",
					"electrical_equipment": "$Appareillage_group",	
					"collective-rehab": "$Reeduc_collective_group",
					"bilans": "$Bilans_group",	
					"physiotherapy": "$PhysioT_group",
					"balneotherapy": "$BalneoT_group",
	}}},

    {$project: {
				"Reeduc_meca": false,
				"Reeduc_sensoriM": false,
				"Reeduc_neuropsy": false,
				"Reeduc_cardioresp": false,
				"Reeduc_nutri": false,
				"Reeduc_urosphinc": false,
				"Readap_reins": false,
				"Appareillage": false,
				"Reeduc_collective": false,
				"Bilans": false,
				"PhysioT": false,
				"BalneoT": false,
				"Reeduc_meca_group": false,
				"Reeduc_sensoriM_group": false,
				"Reeduc_neuropsy_group": false,
				"Reeduc_cardioresp_group": false,
				"Reeduc_nutri_group": false,
				"Reeduc_urosphinc_group": false,
				"Readap_reins_group": false,
				"Appareillage_group": false,
				"Reeduc_collective_group": false,
				"Bilans_group": false,
				"PhysioT_group": false,
				"BalneoT_group": false
	}},



	{$addFields: {"x7_associated_diagnosis": [ 
					"$DAs1", "$DAs2", "$DAs3", "$DAs4", "$DAs5", 
					"$DAs6", "$DAs7", "$DAs8", "$DAs9", "$DAs10", 
					"$DAs11", "$DAs12",	"$DAs13", "$DAs14", "$DAs15", 
					"$DAs16", "$DAs17", "$DAs18", "$DAs19", "$DAs20" 
	]}},


    {$project: {
				"DAs1": false, "DAs2": false, "DAs3": false, "DAs4": false, 
				"DAs5": false, "DAs6": false, "DAs7": false, "DAs8": false, 
				"DAs9": false, "DAs10": false, "DAs11": false, "DAs12": false,		
				"DAs13": false, "DAs14": false, "DAs15": false, "DAs16": false, 
				"DAs17": false, "DAs18": false, "DAs19": false, "DAs20": false
	}},



	{$addFields: {"x8_primary_morbidity": {
					"care_purpose": "$Fin_princ_PC",
					"morbidity": "$morbid_princ",
					"etiology": "$affect_etiol",
					"major_diagnostic_categories": "$cmc_rhs",
					"homogeneous_diagnostic_categories": "$ghj_rhs",
	}}},

    {$project: {
				"Fin_princ_PC": false,
				"morbid_princ": false,
				"affect_etiol": false,
				"cmc_rhs": false,
				"ghj_rhs": false
	}},



	{$addFields: {"x9_clinical_procedures": [
					"$actes.CodActe"  
	]}},


    {$project: {
                "actes": false,
                "Nb_actes": false
    }},




	{$addFields: {"x10_destination": {
					"last_week": "$semaine_fin",
					"output_mode": "$mode_sortie",
					"destination": "$destination"
    }}},

	{$project: {
				"semaine_fin": false,
				"mode_sortie": false,
				"destination": false
	}},



    //Deleted all the features no used to build the CDA
    {$project: {
				"v_genrha": false,
				"clas_rhs": false,
				"Code_retour_rhs": false,
				"ghjrhs": false,
				"clas_rha": false,
				"cmc_rha": false,
				"ghj_rha": false,
				"Code_retour_rha": false,
				"ghjrha": false,
				"presence_sem": false,
				"presence_we": false,
				"diag_princ_categ": false,
	}},


	{$out: "result"}
])


print(db.result.find().count())

db.result.createIndex({Idnum:1})

db.result.renameCollection("ssr2008_aggregate_entity_group")

