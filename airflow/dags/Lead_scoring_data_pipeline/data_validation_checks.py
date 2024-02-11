"""
Import necessary modules
############################################################################## 
"""

import pandas as pd
import sqlite3
from sqlite3 import Error

from Lead_scoring_data_pipeline.constants import *
from Lead_scoring_data_pipeline.schema import raw_data_schema, model_input_schema

###############################################################################
# Define function to validate raw data's schema
############################################################################### 

def raw_data_schema_check():
    '''
    This function check if all the columns mentioned in schema.py are present in
    leadscoring.csv file or not.

   
    INPUTS
        DATA_DIRECTORY : path of the directory where 'leadscoring.csv' 
                        file is present
        raw_data_schema : schema of raw data in the form oa list/tuple as present 
                          in 'schema.py'

    OUTPUT
        If the schema is in line then prints 
        'Raw datas schema is in line with the schema present in schema.py' 
        else prints
        'Raw datas schema is NOT in line with the schema present in schema.py'

    
    SAMPLE USAGE
        raw_data_schema_check
    '''
    lead_scoring_raw_data_path = DATA_DIRECTORY + 'leadscoring.csv'
    ls_data = pd.read_csv(lead_scoring_raw_data_path)
    
    common_columns = ls_data.columns.intersection(raw_data_schema)
    
    if len(common_columns) == len(raw_data_schema):
        print('Raw data schema is in line with the schema present in schema.py')
    else:
        print('Raw datas schema is NOT in line with the schema present in schema.py')
    

###############################################################################
# Define function to validate model's input schema
############################################################################### 

def model_input_schema_check():
    '''
    This function check if all the columns mentioned in model_input_schema in 
    schema.py are present in table named in 'model_input' in db file.

   
    INPUTS
        DB_FILE_NAME : Name of the database file
        DB_PATH : path where the db file should be present
        model_input_schema : schema of models input data in the form oa list/tuple
                          present as in 'schema.py'

    OUTPUT
        If the schema is in line then prints 
        'Models input schema is in line with the schema present in schema.py'
        else prints
        'Models input schema is NOT in line with the schema present in schema.py'
    
    SAMPLE USAGE
        raw_data_schema_check
    '''
    db_file_path = DB_PATH + DB_FILE_NAME
    db_conn = None
    try:
        db_conn = sqlite3.connect(db_file_path)
        model_data = pd.read_sql('SELECT * FROM model_input', con=db_conn)
        
        common_columns = model_data.columns.intersection(model_input_schema)
        
        if len(model_input_schema) == len(common_columns):
            print('Models input schema is in line with the schema present in schema.py')
        else:
            print('Models input schema is NOT in line with the schema present in schema.py')
    except Error as e:
            print(e)
            return "Error"
    finally:
        if db_conn:
            db_conn.close()