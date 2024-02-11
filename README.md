# MLOpsAssignment - Lead Scoring CodePro
Lead Scoring in CodePro


## Table of Contents

* [General Info](#general-information)
* [Project](#project)
* [Technologies Used](#technologies-used)
* [Conclusion](#conclusion)
* [Contact](#contact)

## General Information

The main objectives of lead scoring are as follows:

Remove Junk by categorising leads on the basis of propensity to purchase
Gain insights to streamline lead conversion and address improper targeting
We have chosen L2AC (Leads to Application Completion) as our business metric, as choosing L2P (Leads to Payment) will aggressively drop the leads.
A lead is generated when any person visits CodePro’s website and enters their contact details on the platform. A junk lead is generated when a person who shares their contact details has no interest in the product/service.


Having junk leads in the pipeline creates significant inefficiency in the sales process. Thus, the goal of the data science team is to build a system that categorises leads based on the likelihood of their purchasing CodePro’s course. This system will help remove the inefficiency caused by junk leads in the sales process.


## Project

The solution is to prepare a set of MLOps pipelines that will constantly track the drift changes in data along with predicting the potential leads.

### Following pipelines are created:

1. **Data Pipeline** - To clean the RAW data to be able to make it ready to be learnt.

2. **Training Pipeline** - To learn the cleaned data and perform Hyper-parameter tuning for optimal accuracy, AUC, precision and recall.

3. **Inference Pipeline** - To process un-seen data (in bulk) and predict. Also, to keep an eye on the % of app_registratino_completes.


## Technologies Used

* Airflow
* mlflow
* pandas
* scikit-learn
* sqlite3
* Python

## Conclusion

*  The Airflow pipelines are scheduled so as to learn regularly and keep a track of the accuracy of the model.

## Contact

Created by [@omkar457]
