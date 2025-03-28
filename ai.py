import streamlit as st
import pandas as pd
import boto3
from io import BytesIO
from sqlalchemy import create_engine
import openai

st.title("Análisis Económico")
st.subheader("Prompt 1")

engine = create_engine(st.secrets["mysql_connection"])


query = "SELECT rfc, name FROM clients"
dclients = pd.read_sql(query, engine)
engine.dispose()

dc = pd.DataFrame({'CLIENT': dclients['name']})

client_name = st.sidebar.selectbox("Selecciona un cliente:", dc['CLIENT'])
client_chosen = client_name
rfc_key = dclients[dclients['name'] == client_chosen]['rfc'].iloc[0]

aws_access_key = st.secrets["aws_access_key"]
aws_secret_key = st.secrets["aws_secret_key"]
region_name = 'us-east-1'
bucket_name = 'creafyn'
file_key = 'DescripcionEmpresas_BOT.xlsx'

s3_client = boto3.client(
    's3',
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key,
    region_name=region_name
)

response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
file_stream = BytesIO(response['Body'].read())
df = pd.read_excel(file_stream, engine='openpyxl')

df_seleccionado = df.iloc[:, [1, 3]]
df_seleccionado.columns = ['RFC', 'DESCRIPCION']
descripcion_row = df_seleccionado[df_seleccionado['RFC'] == rfc_key]

if descripcion_row.empty:
    descripcion_text = "No se encontró descripción de la empresa."
else:
    descripcion_text = descripcion_row.iloc[0]['DESCRIPCION']

# 4. Crear el prompt
prompt = f"""
Considerando que la empresa pertenece a la industria de: {descripcion_text}

Analiza el panorama económico actual con base en los datos macroeconómicos más recientes de México
y dime cómo pudiera afectar el panorama macroeconómico a las ventas de la empresa.

Además, mencióname a qué variables macroeconómicas es más sensible la empresa y por qué.

Evalúa también la situación económica actual/esperada en su región y a nivel nacional,
y explica cómo factores como inflación, tasas de interés, tipo de cambio u otros pueden impactar sus ventas.

Proporciona ejemplos específicos y análisis por industria.

Utiliza fuentes confiables como INEGI, Banxico, Data México, Statista, McKinsey, PwC, Deloitte, KPMG, Bain, EY, SEC, Banco Mundial, FMI, OCDE, Concamin, Canacintra, AMIA, ABM, BBVA Research, Santander, El Economista, Expansión, Forbes, México Industry, Secretaria de Economía, SEC:Edgar, IMF, etc.

Para este análisis considera el año 2025, febrero 2025.
"""

openai_api_key = st.secrets["openai_api_key"]
client = openai.OpenAI(api_key=openai_api_key)

response = client.chat.completions.create(
    model="gpt-4o",
    messages=[
        {"role": "system", "content": "Eres un economista experto en análisis macroeconómico y riesgos financieros."},
        {"role": "user", "content": prompt}
    ],
    temperature=0.7
)

col1, col2 = st.columns(2)

with col1:
    st.markdown("### Prompt enviado:")
    st.code(prompt, language="markdown")

with col2:
    st.markdown("### Respuesta generada por ChatGPT:")
    st.write(response.choices[0].message.content)

