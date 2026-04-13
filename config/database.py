from sqlalchemy import create_engine

DB_URL = "postgresql+psycopg2://postgres:postgres@localhost:5432/gkomunika"

def get_engine():
    return create_engine(DB_URL)