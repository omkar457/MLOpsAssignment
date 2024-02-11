'''
filename: utils.py
functions: encode_features, load_model
creator: shashank.gupta
version: 1
'''

###############################################################################
# Import necessary modules
# ##############################################################################

import mlflow
import mlflow.sklearn
import pandas as pd

import sqlite3
from sqlite3 import Error

import os
import logging

from datetime import datetime

from Lead_scoring_inference_pipeline.constants import *

import importlib
import importlib.util

###############################################################################
# Define the function to train the model
# ##############################################################################


def encode_features():
    '''
    This function one hot encodes the categorical features present in our  
    training dataset. This encoding is needed for feeding categorical data 
    to many scikit-learn models.

    INPUTS
        db_file_name : Name of the database file 
        db_path : path where the db file should be
        ONE_HOT_ENCODED_FEATURES : list of the features that needs to be there in the final encoded dataframe
        FEATURES_TO_ENCODE: list of features  from cleaned data that need to be one-hot encoded
        **NOTE : You can modify the encode_featues function used in heart disease's inference
        pipeline for this.

    OUTPUT
        1. Save the encoded features in a table - features

    SAMPLE USAGE
        encode_features()
    '''
    db_file_path = DB_PATH + DB_FILE_NAME
    db_conn = None

    try:
        print(f"Connecting to the database {db_file_path}")
        db_conn = sqlite3.connect(db_file_path)
        #inference_data = pd.read_csv(inference_csv_path, index_col=[0])
        inference_data = pd.read_sql('SELECT * FROM model_input', con=db_conn)
        
        print("Reading from file is Complete!")

        data = inference_data.copy()
        
        for f in FEATURES_TO_ENCODE:
            if(f in data.columns):
                encoded = pd.get_dummies(data[f])
                encoded = encoded.add_prefix(f + '_')
                data = pd.concat([data, encoded], axis=1)
            else:
                print('Feature not found')

        data.drop(columns=FEATURES_TO_ENCODE, inplace = True)

        data_subset = data[ONE_HOT_ENCODED_FEATURES]

        data_subset.to_sql(name='features', con=db_conn, if_exists='replace', index=False)

        db_conn.close()
        db_conn = None
        
    except Error as e:
        print(e)
    finally:
        if db_conn:
            db_conn.close()

###############################################################################
# Define the function to load the model from mlflow model registry
# ##############################################################################

def load_model():
    '''
    This function loads the model which is in production from mlflow registry and 
    uses it to do prediction on the input dataset. Please note this function will the load
    the latest version of the model present in the production stage. 

    INPUTS
        db_file_name : Name of the database file
        db_path : path where the db file should be
        model from mlflow model registry
        model name: name of the model to be loaded
        stage: stage from which the model needs to be loaded i.e. production


    OUTPUT
        Store the predicted values along with input data into a table

    SAMPLE USAGE
        load_model()
    '''
    
    db_file_path = DB_PATH + DB_FILE_NAME
    db_conn = None

    try:
        mlflow.set_tracking_uri(TRACKING_URI)
    
        print(f"Connecting to the database {db_file_path}")
        
        db_conn = sqlite3.connect(db_file_path)
        
        print(f"Reading features from the database {db_file_path}")
        inference_data = pd.read_sql('SELECT * FROM features', con=db_conn)

        model = mlflow.sklearn.load_model(model_uri=f"models:/{MODEL_NAME}/{STAGE}")
        predicted_data = pd.DataFrame(columns=['app_complete_flag'])
        
        predicted_data['app_complete_flag'] = model.predict(inference_data)
        predicted_data.to_sql(name='target', con=db_conn, if_exists='replace', index=False)

        db_conn.close()
        db_conn = None

    except Error as e:
        print(e)
    finally:
        if db_conn:
            db_conn.close()

###############################################################################
# Define the function to check the distribution of output column
# ##############################################################################

def prediction_col_check():
    '''
    This function calculates the % of 1 and 0 predicted by the model and  
    and writes it to a file named 'prediction_distribution.txt'.This file 
    should be created in the ~/airflow/dags/Lead_scoring_inference_pipeline 
    folder. 
    This helps us to monitor if there is any drift observed in the predictions 
    from our model at an overall level. This would determine our decision on 
    when to retrain our model.
    

    INPUTS
        db_file_name : Name of the database file
        db_path : path where the db file should be

    OUTPUT
        Write the output of the monitoring check in prediction_distribution.txt with 
        timestamp.

    SAMPLE USAGE
        prediction_col_check()
    '''
    
    db_file_path = DB_PATH + DB_FILE_NAME
    db_conn = None

    try:
        mlflow.set_tracking_uri(TRACKING_URI)
    
        print(f"Connecting to the database {db_file_path}")
        
        db_conn = sqlite3.connect(db_file_path)
        inference_data = pd.read_sql('SELECT * FROM target', con=db_conn)
        db_conn.close()
        db_conn = None

        count_of_pred_value_1 = len(inference_data[inference_data.app_complete_flag == 1])
        ratio = round(count_of_pred_value_1/inference_data.shape[0], 3)

        with open(FILE_PATH, 'a') as file:
            current_time_stamp = datetime.today().strftime('%Y-%b-%d %H:%M:%S')
            file.write(f"{current_time_stamp} - {ratio}\n")
            file.close()
    
    except Error as e:
        print(e)
    finally:
        if db_conn:
            db_conn.close()
        
    

###############################################################################
# Define the function to check the columns of input features
# ##############################################################################


def input_features_check():
    '''
    This function checks whether all the input columns are present in our new
    data. This ensures the prediction pipeline doesn't break because of change in
    columns in input data.

    INPUTS
        db_file_name : Name of the database file
        db_path : path where the db file should be
        ONE_HOT_ENCODED_FEATURES: List of all the features which need to be present
        in our input data.

    OUTPUT
        It writes the output in a log file based on whether all the columns are present
        or not.
        1. If all the input columns are present then it logs - 'All the models input are present'
        2. Else it logs 'Some of the models inputs are missing'

    SAMPLE USAGE
        input_col_check()
    '''

    db_file_path = DB_PATH + DB_FILE_NAME
    db_conn = None

    try:
        logging.basicConfig(filename="inference_pipe_exec.log",
                    format='%(asctime)s %(message)s',
                    filemode='a')
        logger = logging.getLogger()
        logger.info('Started input features check')

        print(f"Connecting to the database {db_file_path}")
        
        db_conn = sqlite3.connect(db_file_path)
        
        inference_data = pd.read_sql('SELECT * FROM features', con=db_conn)
        
        common_features = inference_data.columns.intersection(ONE_HOT_ENCODED_FEATURES)
        
        if len(ONE_HOT_ENCODED_FEATURES) == len(common_features):
            logger.info('All the models input are present')
        else:
            logger.info('Some of the models inputs are missing')
    
        db_conn.close()
        db_conn = None

    except Error as e:
        print(e)
    finally:
        if db_conn:
            db_conn.close()
            
def module_from_file(module_name, file_path):
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
