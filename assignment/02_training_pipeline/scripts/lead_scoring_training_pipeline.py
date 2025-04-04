##############################################################################
# Import necessary modules
# #############################################################################
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from datetime import datetime, timedelta

from utils import *

###############################################################################
# Define default arguments and DAG
# ##############################################################################
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022,7,30),
    'retries' : 1, 
    'retry_delay' : timedelta(seconds=5)
}


ML_training_dag = DAG(
                dag_id = 'Lead_scoring_training_pipeline',
                default_args = default_args,
                description = 'Training pipeline for Lead Scoring System',
                schedule_interval = '@monthly',
                catchup = False
)

###############################################################################
# Create a task for encode_features() function with task_id 'encoding_categorical_variables'
# ##############################################################################

encoding_categorical_variables_task = PythonOperator(task_id='encoding_categorical_variables', dag=ML_training_dag, python_callable=encode_features)

###############################################################################
# Create a task for get_trained_model() function with task_id 'training_model'
# ##############################################################################

training_model_task = PythonOperator(task_id='training_model', dag=ML_training_dag, python_callable=get_trained_model)

###############################################################################
# Define relations between tasks
# ##############################################################################

encoding_categorical_variables_task >> training_model_task