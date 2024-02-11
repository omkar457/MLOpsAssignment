DB_PATH = '/home/Databases/'
DB_FILE_NAME = 'lead_scoring_data_cleaning.db'

DB_FILE_MLFLOW = 'Lead_scoring_mlflow_production.db'

FILE_PATH = '/home/airflow/dags/Lead_scoring_inference_pipeline/prediction_distribution.txt'
#FILE_PATH = './assignment/03_inference_pipeline/data/leadscoring_inference.csv'

TRACKING_URI = 'http://localhost:6006'

# experiment, model name and stage to load the model from mlflow model registry
MODEL_NAME = 'LightGBM'
STAGE = 'production'
EXPERIMENT = 'Lead_scoring_mlflow_production'

# list of the features that needs to be there in the final encoded dataframe
ONE_HOT_ENCODED_FEATURES = ['total_leads_droppped', 'city_tier', 'referred_lead_1.0', 'first_platform_c_others', 'first_platform_c_Level8', 'first_platform_c_Level0', 'first_platform_c_Level2', 'first_platform_c_Level7', 'first_platform_c_Level1', 'first_utm_source_c_Level6']

# list of features that need to be one-hot encoded
FEATURES_TO_ENCODE = ['first_platform_c', 'first_utm_medium_c', 'first_utm_source_c', 'referred_lead']

INFERENCE_SAMPLE_FILE_PATH = '/home/assignment/03_inference_pipeline/data/leadscoring_inference.csv'
