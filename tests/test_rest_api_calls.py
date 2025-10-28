"""Test the REST API calls."""

import pytest
from fastapi.testclient import TestClient

from API.REST import rest_api_calls


@pytest.fixture
def client():
    """Create a TestClient for the FastAPI app."""
    return TestClient(rest_api_calls.app)


def test_list_tables(client):
    """Test the /tables endpoint."""
    response = client.get("/tables")
    assert response.status_code == 200
    tables = response.json()
    assert isinstance(tables, list), "Response should be a list of table names"
    assert "train" in tables, "'people' table should exist in the database"


def test_get_data(client):
    """Test the /data/{table_name} endpoint."""
    response = client.get("/data/water_potability")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list), "Response should be a list of records"
    assert len(data) == 2011, "There should be 2011 records in the 'water_potability' table"
    assert data[0]["ph"] == 8.316765884214679, "First record's 'ph' value does not match"

def test_get_column_data(client):
    """Test the /data/{table_name}/{column_name} endpoint."""
    response = client.get("/data/train/select/address")
    assert response.status_code == 200
    column_data = response.json()
    assert isinstance(column_data, list), "Response should be a list of column values"
    assert len(column_data) == 29451, "There should be 891 records in the 'age' column of 'train' table"
    assert column_data[0] == "Ksfc Layout,Bangalore", "First record's 'age' value does not match"

def test_get_table_record_count(client):
    """Test that the number of records in a table matches expected count."""
    response = client.get("/data/population_by_country_2020/count")
    assert response.status_code == 200
    data = response.json()
    assert data == 201, "There should be 235 records in the 'population_by_country_2020' table"

if __name__ == "__main__":
    pytest.main()
