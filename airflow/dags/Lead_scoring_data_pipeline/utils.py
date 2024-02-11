##############################################################################
# Import necessary modules and files
##############################################################################

import warnings
warnings.filterwarnings("ignore")

from Lead_scoring_data_pipeline.constants import *

import pandas as pd
import os
import sqlite3
from sqlite3 import Error

#from significant_categorical_level import *
from Lead_scoring_data_pipeline.mapping import * 

import importlib

from Lead_scoring_data_pipeline.mapping.city_tier_mapping import *
from Lead_scoring_data_pipeline.mapping.significant_categorical_level import *

###############################################################################
# Define the function to build database
###############################################################################

def build_dbs():
    '''
    This function checks if the db file with specified name is present 
    in the /Assignment/01_data_pipeline/scripts folder. If it is not present it creates 
    the db file with the given name at the given path. 


    INPUTS
        DB_FILE_NAME : Name of the database file 'utils_output.db'
        DB_PATH : path where the db file should exist  


    OUTPUT
    The function returns the following under the given conditions:
        1. If the file exists at the specified path
                prints 'DB Already Exists' and returns 'DB Exists'

        2. If the db file is not present at the specified loction
                prints 'Creating Database' and creates the sqlite db 
                file at the specified path with the specified name and 
                once the db file is created prints 'New DB Created' and 
                returns 'DB created'


    SAMPLE USAGE
        build_dbs()
    '''
    db_file_path = DB_PATH + DB_FILE_NAME

    if os.path.isfile(db_file_path):
        print( "DB Already Exists")
        print(os.getcwd())
        return "DB Exists"
    else:
        print(f"Creating Database in path {db_file_path}")
        """Creating a database connection to SQLite3"""
        db_conn = None
        try:
            db_conn = sqlite3.connect(db_file_path)
            print(f"Database is created at {db_file_path}")
        except Error as e:
            print(e)
            return "Error"
        finally:
            if db_conn:
                db_conn.close()
                return "DB Created"



###############################################################################
# Define function to load the csv file to the database
###############################################################################

def load_data_into_db():
    '''
    Thie function loads the data present in data directory into the db
    which was created previously.
    It also replaces any null values present in 'toal_leads_dropped' and
    'referred_lead' columns with 0.


    INPUTS
        DB_FILE_NAME : Name of the database file
        DB_PATH : path where the db file should be
        DATA_DIRECTORY : path of the directory where 'leadscoring.csv' 
                        file is present
        

    OUTPUT
        Saves the processed dataframe in the db in a table named 'loaded_data'.
        If the table with the same name already exsists then the function 
        replaces it.


    SAMPLE USAGE
        load_data_into_db()
    '''
    db_file_path = DB_PATH + DB_FILE_NAME

    lead_scoring_raw_data_path = DATA_DIRECTORY + INPUT_CSV_FILE #'leadscoring.csv'
    try:
        db_conn = sqlite3.connect(db_file_path)
        
        print(f"Connected to the database and started loading the data from @{lead_scoring_raw_data_path}")

        ls_data = pd.read_csv(lead_scoring_raw_data_path)

        print(f"Data Loaded")

        ls_data['total_leads_droppped'].fillna(0, inplace=True)
        ls_data['referred_lead'].fillna(0, inplace=True)

        ls_data.to_sql(name='loaded_data', con=db_conn, if_exists='replace', index=False)
    except Error as e:
            print(e)
            return "Error"
    finally:
        if db_conn:
            db_conn.close()


###############################################################################
# Define function to map cities to their respective tiers
###############################################################################

    
def map_city_tier():
    '''
    This function maps all the cities to their respective tier as per the
    mappings provided in the city_tier_mapping.py file. If a
    particular city's tier isn't mapped(present) in the city_tier_mapping.py 
    file then the function maps that particular city to 3.0 which represents
    tier-3.


    INPUTS
        DB_FILE_NAME : Name of the database file
        DB_PATH : path where the db file should be
        city_tier_mapping : a dictionary that maps the cities to their tier

    
    OUTPUT
        Saves the processed dataframe in the db in a table named
        'city_tier_mapped'. If the table with the same name already 
        exsists then the function replaces it.

    
    SAMPLE USAGE
        map_city_tier()

    '''
    db_file_path = DB_PATH + DB_FILE_NAME

    try:
        db_conn = sqlite3.connect(db_file_path)

        """ 1. Read the mapping information from the city_tier.py file"""

        #map_location = '/home/airflow/dags/Lead_scoring_data_pipeline/mapping/city_tier.py'

        #print(f"Loading the Maps data from {map_location}")
        #map_city_spec = importlib.util.spec_from_file_location('Maps', map_location)
        #Maps = importlib.util.module_from_spec(map_city_spec)
        #map_city_spec.loader.exec_module(Maps)

        #from Maps.city_tier import city_tier_mapping
        #Maps.city_tier.city_tier_mapping

        """#2. Read the data from the loaded_data table in database file location: DB_PATH + DB_FILE_NAME"""

        print(f"Read the data from the 'loaded_data' table in database file location: {db_file_path}")
        loaded_data = pd.read_sql('SELECT * FROM loaded_data', con=db_conn)

        """#3. Update the data of the loaded_data with the city_tier details mapped as per the Details."""

        print(f"Update the data of the loaded_data with the city_tier details mapped as per the Details.")

        #city_tier_mapping = Maps.city_tier_mapping  #Module loaded dynamically as the abs. path is outside.

        loaded_data["city_tier"] = loaded_data["city_mapped"].map(city_tier_mapping)
        loaded_data["city_tier"] = loaded_data["city_tier"].fillna(3.0)

        #map_city.city_tier_mapping

        """#4. Save the information as a new table city_tier_mapped in the database."""
        
        print(f"Save the information as a new table city_tier_mapped in the database.")
        loaded_data.to_sql(name='city_tier_mapped', con=db_conn, if_exists='replace', index=False)

    except Error as e:
            print(e)
            return "Error"
    finally:
        if db_conn:
            db_conn.close()
###############################################################################
# Define function to map insignificant categorial variables to "others"
###############################################################################


def map_categorical_vars():
    '''
    This function maps all the insignificant variables present in 'first_platform_c'
    'first_utm_medium_c' and 'first_utm_source_c'. The list of significant variables
    should be stored in a python file in the 'significant_categorical_level.py' 
    so that it can be imported as a variable in utils file.
    

    INPUTS
        DB_FILE_NAME : Name of the database file
        DB_PATH : path where the db file should be present
        list_platform : list of all the significant platform.
        list_medium : list of all the significat medium
        list_source : list of all rhe significant source

        **NOTE : list_platform, list_medium & list_source are all constants and
                 must be stored in 'significant_categorical_level.py'
                 file. The significant levels are calculated by taking top 90
                 percentils of all the levels. For more information refer
                 'data_cleaning.ipynb' notebook.
  

    OUTPUT
        Saves the processed dataframe in the db in a table named
        'categorical_variables_mapped'. If the table with the same name already 
        exsists then the function replaces it.

    
    SAMPLE USAGE
        map_categorical_vars()
    '''
    db_file_path = DB_PATH + DB_FILE_NAME

    try:
        db_conn = sqlite3.connect(db_file_path)

        """#1. Read the data from the city_tier_mapped table in database file location: DB_PATH + DB_FILE_NAME"""

        print(f"Read the data from the 'city_tier_mapped' table in database file location: {db_file_path}")
        loaded_data = pd.read_sql('SELECT * FROM city_tier_mapped', con=db_conn)

        """#2. Update the data of the city_tier_mapped with the 'first_platform_c', 'first_utm_medium_c' and 'first_utm_source_c' details mapped as per the Details."""

        print(f"Update the data of the city_tier_mapped with the 'first_platform_c', 'first_utm_medium_c' and 'first_utm_source_c' details mapped as per the Details.")

        # all the levels below 90 percentage are assgined to a single level called others
        new_df = loaded_data[~loaded_data['first_platform_c'].isin(list_platform)] # get rows for levels which are not present in list_platform
        new_df['first_platform_c'] = "others" # replace the value of these levels to others
        old_df = loaded_data[loaded_data['first_platform_c'].isin(list_platform)] # get rows for levels which are present in list_platform
        df = pd.concat([new_df, old_df]) # concatenate new_df and old_df to get the final dataframe

        # all the levels below 90 percentage are assgined to a single level called others
        new_df = df[~df['first_utm_medium_c'].isin(list_medium)] # get rows for levels which are not present in list_medium
        new_df['first_utm_medium_c'] = "others" # replace the value of these levels to others
        old_df = df[df['first_utm_medium_c'].isin(list_medium)] # get rows for levels which are present in list_medium
        df = pd.concat([new_df, old_df]) # concatenate new_df and old_df to get the final dataframe

        # all the levels below 90 percentage are assgined to a single level called others
        new_df = df[~df['first_utm_source_c'].isin(list_source)] # get rows for levels which are not present in list_source
        new_df['first_utm_source_c'] = "others" # replace the value of these levels to others
        old_df = df[df['first_utm_source_c'].isin(list_source)] # get rows for levels which are present in list_source
        df = pd.concat([new_df, old_df]) # concatenate new_df and old_df to get the final dataframe

        #map_city.city_tier_mapping

        """#4. Save the information as a new table categorical_variables_mapped in the database."""
        
        print(f"Save the information as a new table categorical_variables_mapped in the database.")
        df.to_sql(name='categorical_variables_mapped', con=db_conn, if_exists='replace', index=False)

    except Error as e:
            print(e)
            return "Error"
    finally:
        if db_conn:
            db_conn.close()


##############################################################################
# Define function that maps interaction columns into 4 types of interactions
##############################################################################
def interactions_mapping():
    '''
    This function maps the interaction columns into 4 unique interaction columns
    These mappings are present in 'interaction_mapping.csv' file. 


    INPUTS
        DB_FILE_NAME: Name of the database file
        DB_PATH : path where the db file should be present
        INTERACTION_MAPPING : path to the csv file containing interaction's
                                   mappings
        INDEX_COLUMNS_TRAINING : list of columns to be used as index while pivoting and
                                 unpivoting during training
        INDEX_COLUMNS_INFERENCE: list of columns to be used as index while pivoting and
                                 unpivoting during inference
        NOT_FEATURES: Features which have less significance and needs to be dropped
                                 
        NOTE : Since while inference we will not have 'app_complete_flag' which is
        our label, we will have to exculde it from our features list. It is recommended 
        that you use an if loop and check if 'app_complete_flag' is present in 
        'categorical_variables_mapped' table and if it is present pass a list with 
        'app_complete_flag' column, or else pass a list without 'app_complete_flag'
        column.

    
    OUTPUT
        Saves the processed dataframe in the db in a table named 
        'interactions_mapped'. If the table with the same name already exsists then 
        the function replaces it.
        
        It also drops all the features that are not requried for training model and 
        writes it in a table named 'model_input'

    
    SAMPLE USAGE
        interactions_mapping()
    '''
    
    db_file_path = DB_PATH + DB_FILE_NAME

    try:
        db_conn = sqlite3.connect(db_file_path)

        """ 1. Read the mapping information from the interaction_mapping.csv file"""

        print(f"Loading the Maps data from {INTERACTION_MAPPING}")
        # read the interaction mapping file
        df_event_mapping = pd.read_csv(INTERACTION_MAPPING, index_col=[0])

        #from Maps.city_tier import city_tier_mapping
        #Maps.city_tier.city_tier_mapping

        """#2. Read the data from the categorical_variables_mapped table in database file location: DB_PATH + DB_FILE_NAME"""

        print(f"Read the data from the 'categorical_variables_mapped' table in database file location: {db_file_path}")
        loaded_data = pd.read_sql('SELECT * FROM categorical_variables_mapped', con=db_conn)

        """#3. Update the data of the categorical_variables_mapped with the interaction mapping data."""

        print(f"Update the data of the categorical_variables_mapped with the interaction mapping data.")

        index_columns = INDEX_COLUMNS_TRAINING

        if 'app_complete_flag' in loaded_data.columns:
            index_columns = INDEX_COLUMNS_TRAINING
        else:
            index_columns = INDEX_COLUMNS_INFERENCE

        # unpivot the interaction columns and put the values in rows
        df_unpivot = pd.melt(loaded_data, id_vars=index_columns, var_name='interaction_type', value_name='interaction_value')

        df_unpivot['interaction_value'] = df_unpivot['interaction_value'].fillna(0)

        # map interaction type column with the mapping file to get interaction mapping
        df = pd.merge(df_unpivot, df_event_mapping, on='interaction_type', how='left')

        #dropping the interaction type column as it is not needed
        df = df.drop(['interaction_type'], axis=1)

        # pivoting the interaction mapping column values to individual columns in the dataset
        df_pivot = df.pivot_table(
                values='interaction_value', index=index_columns, columns='interaction_mapping', aggfunc='sum')
        df_pivot = df_pivot.reset_index()

        # the rows are again converted back to columns. 37 rows gets converted to a single row.
        print(f"Shape of the dataset: {df_pivot.shape}")


        """#4. Save the information as a new table model_input in the database."""
        
        print(f"Save the information as a new table model_input in the database.")
        df_pivot.to_sql(name='model_input', con=db_conn, if_exists='replace', index=False)

    except Error as e:
            print(e)
            return "Error"
    finally:
        if db_conn:
            db_conn.close()