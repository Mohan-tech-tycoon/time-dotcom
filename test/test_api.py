import os
import config.src_config as conf

os.environ['KAGGLE_CONFIG_DIR'] = conf.KAGGLE_CONFIG_DIR

# from kaggle.api.kaggle_api_extended import KaggleApi

import kaggle

# api = KaggleApi()
# api.authenticate()

api = kaggle.api
api.authenticate()

file_list = api.dataset_list_files(dataset=conf.KAGGLE_DATASET).files
print("file_list:",file_list)
api.dataset_download_file(
    dataset=conf.KAGGLE_DATASET,
    file_name="chicago_taxi_trips_details.csv",
    path=conf.TEMP_DIR
)



