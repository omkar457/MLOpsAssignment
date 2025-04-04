DB_PATH = '/home/Databases/' #'/home/airflow/dags/Lead_scoring_data_pipeline/'
DB_FILE_NAME = 'lead_scoring_data_cleaning.db'

DB_FILE_MLFLOW = 'Lead_scoring_mlflow_production.db'

TRACKING_URI = 'http://localhost:6006'
EXPERIMENT = 'Lead_scoring_mlflow_production'


# model config imported from pycaret experimentation
model_config = {'boosting_type': 'gbdt',
                 'class_weight': None,
                 'colsample_bytree': 1.0,
                 'importance_type': 'split',
                 'learning_rate': 0.1,
                 'max_depth': -1,
                 'min_child_samples': 20,
                 'min_child_weight': 0.001,
                 'min_split_gain': 0.0,
                 'n_estimators': 100,
                 'n_jobs': -1,
                 'num_leaves': 31,
                 'objective': None,
                 'random_state': 42,
                 'reg_alpha': 0.0,
                 'reg_lambda': 0.0,
                 'silent': 'warn',
                 'subsample': 1.0,
                 'subsample_for_bin': 200000,
                 'subsample_freq': 0,
                 'device': 'gpu'}

# list of the features that needs to be there in the final encoded dataframe
ONE_HOT_ENCODED_FEATURES = ['total_leads_droppped', 'city_tier', 'referred_lead_1.0', 'app_complete_flag', 'first_platform_c_others', 'first_platform_c_Level8', 'first_platform_c_Level0', 'first_platform_c_Level2', 'first_platform_c_Level7', 'first_platform_c_Level1', 'first_utm_source_c_Level6']
# list of features that need to be one-hot encoded
#FEATURES_TO_ENCODE = ['city_tier', 'first_platform_c', 'first_utm_medium_c', 'first_utm_source_c', 'total_leads_droppped', 'referred_lead', 'assistance_interaction', 'career_interaction', 'payment_interaction', 'social_interaction', 'syllabus_interaction']
FEATURES_TO_ENCODE = ['first_platform_c', 'first_utm_medium_c', 'first_utm_source_c', 'referred_lead']
