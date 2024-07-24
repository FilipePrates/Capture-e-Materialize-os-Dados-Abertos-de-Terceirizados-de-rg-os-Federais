import os
import pandas as pd
from sqlalchemy import create_engine
import dash
from dash import dash_table, html
from dotenv import load_dotenv
load_dotenv()

# Table à ser visualizada
tableName = "transformed"

# Define the database connection details
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Create the SQLAlchemy engine
engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

# Function to get the tables from the staging.transformed schema
def get_tables_from_staging(engine):
    query = f"""
    SELECT table_schema, table_name
    FROM information_schema.tables
    WHERE table_schema = 'staging'
    AND table_name = '{tableName}'
    ORDER BY table_schema, table_name;
    """
    tables = pd.read_sql(query, engine)
    return tables

# Function to fetch data from a specific table
def fetch_table_data(engine, table_schema, table_name):
    query = f'SELECT * FROM {table_schema}.{table_name} LIMIT 100'
    data = pd.read_sql(query, engine)
    return data

# Fetch the table data
tables = get_tables_from_staging(engine)
data = pd.DataFrame()

if not tables.empty:
    first_schema = tables.iloc[0]['table_schema']
    first_table = tables.iloc[0]['table_name']
    data = fetch_table_data(engine, first_schema, first_table)

# Create a Dash app
app = dash.Dash(__name__)

# Define the layout of the app
app.layout = html.Div([
    dash_table.DataTable(
        id='table',
        columns=[{"name": i, "id": i} for i in data.columns],
        data=data.to_dict('records'),
        page_size=20,
        style_table={'overflowX': 'auto'},
        style_cell={
            'height': 'auto',
            'minWidth': '100px', 'width': '100px', 'maxWidth': '180px',
            'whiteSpace': 'normal'
        }
    )
])
if __name__ == '__main__':
    print(tables.head(2))
    print(data.head(4))

# Run the app
if __name__ == '__main__':
    app.run_server(debug=True)
