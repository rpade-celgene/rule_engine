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

To run the engine:
  python "/mnt/efs/ipsworkspace/rule_engine/lib/init_engine.py" -v "iqvia" -y "2019"
  
The engine will capture events and generate report and dataset output. There will be available based on the location of the dataset provided.
