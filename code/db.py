from sqlalchemy import create_engine

# -----------------------------
# Database configuration
# -----------------------------
DB_USER = 'root'
DB_PASSWORD = ''
DB_HOST = 'localhost'       # or your host
DB_PORT = '3306'
DB_NAME = 'football_analysis'

connection_string = f'mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
engine = create_engine(connection_string)

