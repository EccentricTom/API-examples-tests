# script that gets data from the web as a csv file and loads it into a database

from pathlib import Path

import pandas as pd
import requests
from sqlalchemy import create_engine
import os

# Function to download a selection of CSV files from the web
def download_csv(url:str) -> str:
    """
    Download a CSV file from a given URL and save it locally.
    
    Args:
        url (str): The URL of the CSV file to download.

    Returns:
        str: The filename of the downloaded CSV file.
    """
    # Download the file from the URL
    response = requests.get(url, allow_redirects=True)
    # Extract the filename from the URL, cleaning any unwanted characters
    filename = url.split("/")[-1].replace("%20(1)", "")
    # Save the file to the current directory
    with Path("data",filename).open('wb') as file:
        file.write(response.content)
    return filename

# Function to load a CSV file as a pandas DataFrame
def load_csv_to_dataframe(file_path: str) -> pd.DataFrame:
    """
    Load a CSV file into a pandas DataFrame. Remove null values.
    
    Args:
        file_path (str): The path to the CSV file.

    Returns:
        pd.DataFrame: The loaded DataFrame.

    """
    df = pd.read_csv(Path("data", file_path))
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

if __name__ == "__main__":
    csv_urls = [
        "https://raw.githubusercontent.com/MainakRepositor/Datasets/refs/heads/master/water_potability.csv",
        "https://raw.githubusercontent.com/MainakRepositor/Datasets/refs/heads/master/train.csv",
        "https://raw.githubusercontent.com/MainakRepositor/Datasets/refs/heads/master/population_by_country_2020%20(1).csv"
    ]
    for csv_url in csv_urls:
        file_name = download_csv(csv_url)
        df = load_csv_to_dataframe(file_name)
        load_dataframe_to_db(df, table_name=file_name.split(".")[0],
                             db_connection_string="sqlite:///data/data.db")