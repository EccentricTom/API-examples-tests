"""Fetch data from the sqlite database using REST API calls."""

import fastapi
import pandas as pd
import uvicorn
from sqlalchemy import create_engine, text

app = fastapi.FastAPI(title="Database REST API", version="0.1")
db_connection_string = "sqlite:///data/data.db"

@app.get("/data/{table_name}")
def get_data(table_name: str):
    """
    Fetch all data from the specified table in the database.

    Args:
        table_name (str): The name of the table to fetch data from.

    Returns:
        list: A list of records from the specified table.

    """
    engine = create_engine(db_connection_string)
    with engine.connect() as connection:
        df = pd.read_sql_table(table_name, con=connection)
        records = df.to_dict(orient="records")
    return records

@app.get("/tables")
def list_tables():
    """
    List all table names in the database.

    Returns:
        list: A list of table names.
        
    """
    engine = create_engine(db_connection_string)
    with engine.connect() as connection:
        result = connection.execute(text(
            "SELECT name FROM sqlite_master WHERE type='table';"
        ))
        tables = [row[0] for row in result.fetchall()]
    return tables

@app.get("/data/{table_name}/{column_name}")
def get_column_data(table_name: str, column_name: str):
    """
    Fetch data from a specific column in the specified table.

    Args:
        table_name (str): The name of the table.
        column_name (str): The name of the column.

    Returns:
        list: A list of values from the specified column.

    """
    engine = create_engine(db_connection_string)
    with engine.connect() as connection:
        df = pd.read_sql_table(table_name, con=connection)
        if column_name not in df.columns:
            return {"error": f"Column '{column_name}' does not exist in table '{table_name}'."}
        column_data = df[column_name].tolist()
    return column_data

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)