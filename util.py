#get the required libraries for the project
from datetime import datetime
from sqlalchemy import create_engine
import glob
import os
from dotenv import dotenv_values
dotenv_values()

#this function is used for file-naming during the ETL process
def file_names():
    name_of_file = datetime.now().strftime('%Y%m%d%H%M%S')
    return name_of_file
# file_names()

#this function is used to define the connection to postgresql
def db_connect(): 
    config= dict(dotenv_values('.env')) #the crendentials from the .env file is defined
    db_user_name= config.get('DB_USER_NAME') #the database username
    db_password= config.get('DB_PASSWORD') #the database password
    db_name= config.get('DB_NAME') #the database name
    db_port= config.get('PORT') #the database port
    db_host= config.get('HOST') #the database host
    #the engine for connection is defined
    conn_ = create_engine(f'postgresql+psycopg2://{db_user_name}:{db_password}@{db_host}:{db_port}/{db_name}')
    return conn_
# db_connect()

#this function is used to read the latest file saved in the staging folder.
def last_saved_file():
    folder_path = r"staging" #the folder name is defined
    file_type = r"*.csv"  # Specify the file type (e.g., "*.csv" for CSV files)
    files = glob.glob(os.path.join(folder_path, file_type))
    latest_file = max(files, key=os.path.getctime)
    return latest_file
# last_saved_file()

#this function is used to read the latest file saved in the transformed folder.
def lastest_transformed_file():
    folder_path = r"transformed" #the folder name is defined
    file_type = r"*.csv"  # Specify the file type (e.g., "*.csv" for CSV files)
    files = glob.glob(os.path.join(folder_path, file_type))
    latest_files = max(files, key=os.path.getctime)
    return latest_files
# lastest_transformed_file()