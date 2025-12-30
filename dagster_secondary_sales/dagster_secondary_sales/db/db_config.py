from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()

# PostgreSQL credentials - use environment variables for security
dbname_pg = os.getenv('dbname_pg')
user_pg = os.getenv('user_pg')
password_pg = os.getenv('password_pg')
host_pg = os.getenv('host_pg')
port_pg = os.getenv('port_pg')

#  Create SQLAlchemy engine
engine_pg = create_engine(f"postgresql://{user_pg}:{password_pg}@{host_pg}:{port_pg}/{dbname_pg}")

#  Optional: Test the connection
try:
    with engine_pg.connect() as connection:
        print("Database connection successful!")
except Exception as e:
    print(f"Database connection failed: {e}")

print(engine_pg)
