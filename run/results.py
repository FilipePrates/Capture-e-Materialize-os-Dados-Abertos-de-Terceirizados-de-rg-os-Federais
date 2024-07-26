import os
import pandas as pd
from sqlalchemy import create_engine
import dash
from dash import dash_table, html, dcc
from dotenv import load_dotenv
load_dotenv()

# Define as configurações de tabelas da visualização
table_configs = [
    {"schema": "staging", "table": "transformed", "label": "staging.transformed"},
    {"schema": "public", "table": "raw", "label": "raw"},
    
    {"schema": "public", "table": "historic_transformed", "label": "staging.historic_transformed"},
    {"schema": "public", "table": "historic_raw", "label": "historic_raw"},

    {"schema": "public", "table": "logs__historic_capture", "label": "logs__historic_capture"},
    {"schema": "public", "table": "logs__historic_materialize", "label": "logs__historic_materialize"},
    {"schema": "public", "table": "logs__capture", "label": "logs__capture"},
    {"schema": "public", "table": "logs__materialize", "label": "logs__materialize"},
]
LIMIT = 300

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
    query = f'SELECT * FROM {table_schema}.{table_name} LIMIT {LIMIT}'
    data = pd.read_sql(query, engine)
    print(f' <> {table_schema}.{table_name} \n', data.head(3))
    
    # Acrescenta uma linha com Tipos das Colunas
    types_query = f"""
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = '{table_schema}' AND table_name = '{table_name}';
    """
    types = pd.read_sql(types_query, engine)
    types_row = {col: dtype for col, dtype in zip(types['column_name'], types['data_type'])}
    data = pd.concat([pd.DataFrame([types_row]), data], ignore_index=True)

    return data

# Função para capturar o número de linhas em cada tabela
def fetch_table_count(engine, table_schema, table_name):
    query = f'SELECT COUNT(*) FROM {table_schema}.{table_name}'
    count = pd.read_sql(query, engine)
    print(count)
    return count
# fetch_table_count(engine, 'staging', 'raw_historic')
# Crie o app Dash
app = dash.Dash(__name__)

# Define the layout with tabs
app.layout = html.Div([
    html.H2("Tabelas armazenadas no PostgreSQL 🌄"),
    dcc.Tabs(id='tabs', value='tab-0', children=[
        dcc.Tab(label=config['label'], value=f"tab-{index}") for index, config in enumerate(table_configs)
    ]),
    html.Div(id='tabs-content')
]) 

# Callback to update table data based on selected tab
@app.callback(
    dash.dependencies.Output('tabs-content', 'children'),
    [dash.dependencies.Input('tabs', 'value')]
)
def render_content(tab):
    index = int(tab.split('-')[1])
    config = table_configs[index]
    try:
        data = fetch_table_data(engine, config['schema'], config['table'])
        return html.Div([
            dash_table.DataTable(
                id='table',
                columns=[{"name": i, "id": i} for i in data.columns],
                data=data.to_dict('records'),
                page_size=20,
                style_table={'overflowX': 'auto'},
                style_cell={
                    'height': 'auto',
                    'minWidth': '100px', 'width': '100px', 'maxWidth': '180px',
                    'whiteSpace': 'normal',
                    'textAlign': 'left'  # Align text to the left
                },
                style_data_conditional=[
                    {
                        'if': {'row_index': 0},  # apply to the first row
                        'backgroundColor': '#b3b3b3',
                        'color': 'white'
                    }
                ]
            )
        ])
    except Exception as e:
        print(e)
        return html.Div(
            "Dados para esta tabela ainda não foram capturados.",
            style={
                'textAlign': 'center',
                'color': '#8b0000',
                'fontSize': '24px',
                'marginTop': '5vh'
            }
        )

if __name__ == '__main__':
    app.run_server(debug=True)
