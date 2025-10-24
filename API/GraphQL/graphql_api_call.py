"""Collect data from database using GraphQL API calls."""

import graphene
from sqlalchemy import create_engine, text

db_connection_string = "sqlite:///data/data.db"

class RecordType(graphene.ObjectType):
    """GraphQL type for a database record."""

    # Dynamically define fields based on the table structure
    pass

class Query(graphene.ObjectType):
    """GraphQL query class to fetch data from the database."""

    records = graphene.List(RecordType, table_name=graphene.String(required=True))
    tables = graphene.List(graphene.String)

    def resolve_records(self, info, table_name):
        """Fetch all records from the specified table."""
        engine = create_engine(db_connection_string)
        with engine.connect() as connection:
            result = connection.execute(text(f"SELECT * FROM {table_name}"))
            rows = result.fetchall()
            # Convert rows to list of RecordType instances
            records = [RecordType(**dict(row)) for row in rows]
        return records

    def resolve_tables(self, info):
        """List all table names in the database."""
        engine = create_engine(db_connection_string)
        with engine.connect() as connection:
            result = connection.execute(text(
                "SELECT name FROM sqlite_master WHERE type='table';"
            ))
            tables = [row[0] for row in result.fetchall()]
        return tables