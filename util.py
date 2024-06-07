from datetime import datetime
from sqlalchemy import create_engine
import psycopg2
import glob
import os
from dotenv import dotenv_values
dotenv_values()


def file_names():
    name_of_file = datetime.now().strftime('%Y%m%d%H%M%S')
    return name_of_file
# file_names()

def db_connect(): 
    config= dict(dotenv_values('.env'))
    db_user_name= config.get('DB_USER_NAME')
    db_password= config.get('DB_PASSWORD')
    db_name= config.get('DB_NAME')
    db_port= config.get('PORT')
    db_host= config.get('HOST')
    return create_engine(f'postgresql+psycopg2://{db_user_name}:{db_password}@{db_host}:{db_port}/{db_name}')
# db_connect()

def last_saved_file():
    folder_path = r"staging"
    file_type = r"*.csv"  # Specify the file type (e.g., "*.csv" for CSV files)
    files = glob.glob(os.path.join(folder_path, file_type))
    latest_file = max(files, key=os.path.getctime)
    return latest_file
# last_saved_file()
