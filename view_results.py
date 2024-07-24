import os
import pandas as pd
from sqlalchemy import create_engine
import dash
from dash import dash_table, html
from dotenv import load_dotenv
load_dotenv()

# Tabela à ser visualizada
schemaName = "staging"
tableName = "transformed"
# schemaName = "public"
# tableName = "raw"
limit = 99

# Defina os parâmetros de conexão com o banco de dados
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Crie SQLAlchemy engine
engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

# Função para capturar da tabela schemaName.tableName
def fetch_table_data(engine, table_schema, table_name):
    query = f'SELECT * FROM {table_schema}.{table_name} LIMIT {limit}'
    data = pd.read_sql(query, engine)
    types_query = f"""
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = '{table_schema}' AND table_name = '{table_name}';
    """
    types = pd.read_sql(types_query, engine)
    
    # Add a row for column types
    types_row = {col: dtype for col, dtype in zip(types['column_name'], types['data_type'])}
    data = pd.concat([pd.DataFrame([types_row]), data], ignore_index=True)
    return data

# Capture os dados da tabela schemaName.tableName
data = fetch_table_data(engine, schemaName, tableName)

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
        },
        style_data_conditional=[
            {
                'if': {'row_index': 0},  # apply to the second row
                'backgroundColor': '#b3b3b3',
                'color': 'white'
            }
        ]
    )
])
if __name__ == '__main__':
    print(data.head(4))

# Run the app
if __name__ == '__main__':
    app.run_server(debug=True)
