"""Collect data from database using GraphQL API calls."""

import graphene
from sqlalchemy import create_engine, text

db_connection_string = "sqlite:///data/data.db"

