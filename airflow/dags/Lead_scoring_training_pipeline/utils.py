'''
filename: utils.py
functions: encode_features, get_train_model
creator: shashank.gupta
version: 1
'''

###############################################################################
# Import necessary modules
# ##############################################################################

import pandas as pd
import numpy as np

import sqlite3
from sqlite3 import Error

import mlflow
import mlflow.sklearn

from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score
import lightgbm as lgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score

from Lead_scoring_training_pipeline.constants import *
import os
#from constants import *

from sklearn.metrics import precision_score, recall_score

###############################################################################
# Define the function to encode features
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
       

    OUTPUT
        1. Save the encoded features in a table - features
        2. Save the target variable in a separate table - target


    SAMPLE USAGE
        encode_features()
        
    **NOTE : You can modify the encode_featues function used in heart disease's inference
        pipeline from the pre-requisite module for this.
    '''

    mlflow_db_file_path = DB_PATH + DB_FILE_MLFLOW

    print(f"Working with MLFlow DB: {mlflow_db_file_path}")

    if os.path.isfile(mlflow_db_file_path):
        print(f"MLFLOW DB Already Exists at {mlflow_db_file_path}")
        print(os.getcwd())
    else:
        mlflow_db_conn = None
        try:
            mlflow_db_conn = sqlite3.connect(mlflow_db_file_path)
        except Error as e:
            print(e)
            return "Error"
        finally:
            if mlflow_db_conn:
                mlflow_db_conn.close()
                mlflow_db_conn = None

            
    db_file_path = DB_PATH + DB_FILE_NAME
    
    db_conn = None
    
    try:
        print(f"Reading the cleaned data from the database {db_file_path}")
        db_conn = sqlite3.connect(db_file_path)
        cleaned_data = pd.read_sql('SELECT * FROM model_input', con=db_conn)
        
        print(f"Reading Complete!")
        
        #one_hot_encoded_data = OneHotEncoder(cleaned_data, categories=FEATURES_TO_ENCODE)
        data = cleaned_data.copy()
        
        for f in FEATURES_TO_ENCODE:
            if(f in data.columns):
                encoded = pd.get_dummies(data[f])
                encoded = encoded.add_prefix(f + '_')
                data = pd.concat([data, encoded], axis=1)
            else:
                print('Feature not found')

        data.drop(columns=FEATURES_TO_ENCODE, inplace = True)

        data_subset = data[ONE_HOT_ENCODED_FEATURES]
        
        target_column = data_subset.pop('app_complete_flag')
        
        data_subset.to_sql(name='features', con=db_conn, if_exists='replace', index=False)
        target_column.to_sql(name='target', con=db_conn, if_exists='replace', index=False)

        db_conn.close()
        db_conn = None
        
    except Error as e:
        print(e)
    finally:
        if db_conn:
            db_conn.close()
    
    
    


###############################################################################
# Define the function to train the model
# ##############################################################################

def get_trained_model():
    '''
    This function setups mlflow experiment to track the run of the training pipeline. It 
    also trains the model based on the features created in the previous function and 
    logs the train model into mlflow model registry for prediction. The input dataset is split
    into train and test data and the auc score calculated on the test data and
    recorded as a metric in mlflow run.   

    INPUTS
        db_file_name : Name of the database file
        db_path : path where the db file should be


    OUTPUT
        Tracks the run in experiment named 'Lead_Scoring_Training_Pipeline'
        Logs the trained model into mlflow model registry with name 'LightGBM'
        Logs the metrics and parameters into mlflow run
        Calculate auc from the test data and log into mlflow run  

    SAMPLE USAGE
        get_trained_model()
    '''
    db_file_path = DB_PATH + DB_FILE_NAME
    db_conn = None
    
    try:
        print(f"Reading the features and target from the database {db_file_path}")
        db_conn = sqlite3.connect(db_file_path)

        features = pd.read_sql('SELECT * FROM features', con=db_conn)
        target_col = pd.read_sql('SELECT * FROM target', con=db_conn)
        
        db_conn.close()
        db_conn = None

        X_train, X_test, y_train, y_test = train_test_split(features, target_col, test_size = 0.3, random_state=42)
        
        mlflow.set_tracking_uri(TRACKING_URI)
        
        
        try:
            # Creating an experiment
            logging.info("Creating mlflow experiment")
            mlflow.create_experiment(EXPERIMENT)
        except:
            pass

        # Setting the environment with the created experiment
        mlflow.set_experiment(EXPERIMENT)
        
        with mlflow.start_run(run_name="run_LGBMClassifier_Without_HPTune") as run:
            lightgbm_model = lgb.LGBMClassifier()
            
            lightgbm_model.set_params(**model_config)                   # model_config from Constants.py
            
            lightgbm_model.fit(X_train, y_train)
            mlflow.log_params(model_config)
            
            y_pred = lightgbm_model.predict(X_test)

            model_info = mlflow.sklearn.log_model(sk_model=lightgbm_model, artifact_path='models', registered_model_name='LightGBM')
            
            acc=accuracy_score(y_pred, y_test)
            mlflow.log_metric("test_accuracy", acc)

            auc = roc_auc_score(y_pred, y_test)
            mlflow.log_metric("auc", auc)
            
            precision = precision_score(y_pred, y_test,average= 'macro')
            mlflow.log_metric("Precision", precision)
            
            recall = recall_score(y_pred, y_test, average= 'macro')
            mlflow.log_metric("Recall", recall)

            runID = run.info.run_uuid
            print("Inside MLflow Run with id {}".format(runID))
            #mlflow.sklearn.save_model(lightgbm_model, model_info.model_uri)

    except Error as e:
        print(e)
    finally:
        if db_conn:
            db_conn.close()
