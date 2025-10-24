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

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)