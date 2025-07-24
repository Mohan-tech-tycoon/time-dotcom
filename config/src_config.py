import os 

ROOT_DIR = "/workspaces/time-dotcom/"
CONFIG_DIR = os.path.join(ROOT_DIR, "config")
TEMP_DIR = os.path.join(ROOT_DIR,"resources", "temp")

KAGGLE_DATASET = r"chicago/chicago-taxi-trips-bq"
KAGGLE_CONFIG_DIR = os.path.join(CONFIG_DIR, ".kaggle")