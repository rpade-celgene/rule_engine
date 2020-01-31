# RULE ENGINE

## Overview
This is a general QC pipeline to check the quality of the raw data within a dataset:
  Missing values
  Negative numeric values
  String numeric values
  Date logical checks

The engine is configuration driven. The config.py file will be the file users will work with the most. 

## Usage 
Example of Config file:

The configuration file is written as a JSON so that it is human readable and simple to make changes to. Within the configuration file, there are two levels that must exist for the qc_engine to run. The top level and the vendor level.
	At the top level, the configuration file should have “spec_year” and “spec_version” and “vendor_name” keys at minimum. 
```json
  {
		“spec_year”: 2019,
		“spec_version”: “Test”,
		“vendor_name”: { }	
	} 
```
	At the vendor level, the configuration files should have “refresh_dt”, “folder”, “database”, “rules”, and “rule_sequence” as keys at the minimum.
```json
  “vendor_name”: {
		“refresh_dt”: “2019-08-01”,
		“folder”: “vendor_folder”,
		“database”: “vendor_hive_database”,
		“rules”: { },
		“rule_sequence”: []
	}
```
	At the rule level, there are 5 key rules.
	1. Duplication("duplication"): Which is key/pair of table name and PK(lst of columns that have uqnique values)
	2. Missing("missing"): Which is a key/pair of table name and columns that you want to check if there are missing values. If a table is missing, the engine will check all the columns for missing values.
	3. String Tables("string_tables"): "value" is the key/value for string to search in column headers. i.e if "value": "value", the engine will search for column headers that have "value" in the string. Whichever tables have have columns that meet requirement will have those columns checked to see if they have strings or negative numbers to flag.
	4. Last Dt/ Death Dt("last_dt"/"death_dt"): Given table/column infortaion regarding a specific date field and unique id, make sure no other date fields have values that come after the date speficied per unique id. 

To run the engine:
  python "/mnt/efs/ipsworkspace/rule_engine/lib/init_engine.py" -v "iqvia" -y "2019"
  
The engine will capture events and generate report and dataset output. There will be available based on the location of the dataset provided.
