"""Test suite for loading data into the database."""
import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from data.load_data_to_db import load_csv_to_dataframe, load_dataframe_to_db


@pytest.fixture
def sample_csv(tmp_path):
    """Create a sample CSV file for testing."""
    csv_content = """id,name,age"""
    csv_content += "\n1,Alice,30"
    csv_content += "\n2,Bob,25"
    csv_content += "\n3,Charlie,35"
    csv_file = tmp_path / "sample.csv"
    csv_file.write_text(csv_content)
    return str(csv_file)

def test_load_csv_to_dataframe(sample_csv):
    """Test loading CSV file into DataFrame."""
    df = load_csv_to_dataframe(sample_csv)
    assert not df.isnull().values.any(), "DataFrame contains null values"
    assert len(df) == 3, "DataFrame should have 3 rows"
    assert list(df.columns) == ["id", "name", "age"], "DataFrame columns do not match"

def test_load_dataframe_to_db(tmp_path):
    """Test loading DataFrame into database."""
    # Create a sample DataFrame
    data = {
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "age": [30, 25, 35]
    }
    df = pd.DataFrame(data)

    # Create a temporary SQLite database
    db_path = tmp_path / "test.db"
    db_connection_string = f"sqlite:///{db_path}"

    # Load DataFrame into the database
    load_dataframe_to_db(df, "people", db_connection_string)

    # Verify data in the database
    engine = create_engine(db_connection_string)
    with engine.connect() as connection:
        result = connection.execute(text("SELECT * FROM people"))
        rows = result.fetchall()
        assert len(rows) == 3, "Database should have 3 rows"
        assert rows[0] == (1, "Alice", 30), "First row does not match"
        assert rows[1] == (2, "Bob", 25), "Second row does not match"
        assert rows[2] == (3, "Charlie", 35), "Third row does not match"