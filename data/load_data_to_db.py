# script that gets data from the web as a csv file and loads it into a database

from pathlib import Path

import pandas as pd
import requests
from sqlalchemy import create_engine


# Function to download a selection of CSV files from the web
def download_csv(url:str) -> None:
    """
    Download a CSV file from a given URL and save it locally.
    
    Args:
        url (str): The URL of the CSV file to download.

    """
    # Download the file from the URL
    response = requests.get(url, allow_redirects=True)
    # Extract the filename from the URL
    filename = url.split("/")[-1]
    # Save the file to the current directory
    with Path(filename).open('wb') as file:
        file.write(response.content)

# Function to load a CSV file as a pandas DataFrame
def load_csv_to_dataframe(file_path: str) -> pd.DataFrame:
    """
    Load a CSV file into a pandas DataFrame. Remove null values.
    
    Args:
        file_path (str): The path to the CSV file.

    Returns:
        pd.DataFrame: The loaded DataFrame.

    """
    df = pd.read_csv(file_path)
    df.dropna(inplace=True)
    return df

# Function to load a DataFrame into a database
def load_dataframe_to_db(df: pd.DataFrame, table_name: str,
                          db_connection_string: str) -> None:
    """
    Load a pandas DataFrame into a database table.
    
    Args:
        df (pd.DataFrame): The DataFrame to load.
        table_name (str): The name of the target database table.
        db_connection_string (str): The database connection string.

    """
    # Create a database engine
    engine = create_engine(db_connection_string)
    # Load the DataFrame into the database
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)