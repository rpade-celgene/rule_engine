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


To run the engine:
  python "/mnt/efs/ipsworkspace/rule_engine/lib/init_engine.py" -v "iqvia" -y "2019"
  
The engine will capture events and generate report and dataset output. There will be available based on the location of the dataset provided.
