import streamlit as st
import pandas as pd
import boto3
from io import BytesIO
from sqlalchemy import create_engine
import openai

st.set_page_config(page_title="Análisis Económico", layout="centered")

st.title("Análisis Económico Personalizado")

engine = create_engine('mysql+pymysql://satws_extractor:LppgQWI22Txqzl1@db-cluster-momento-capital-prod.cluster-c7b6x1wx8cfw.us-east-1.rds.amazonaws.com/momento_capital')
query = "SELECT rfc, name FROM clients"
dclients = pd.read_sql(query, engine)
engine.dispose()

dc = pd.DataFrame({'CLIENT': dclients['name']})

client_chosen = st.selectbox("Selecciona un cliente:", dc['CLIENT'].tolist())

if client_chosen:
    rfc_key = dclients[dclients['name'] == client_chosen]['rfc'].iloc[0]
    st.write(f"**RFC del cliente seleccionado:** {rfc_key}")

    aws_access_key = st.secrets["aws_access_key"]
    aws_secret_key = st.secrets["aws_secret_key"]

    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name='us-east-1'
    )

    response = s3_client.get_object(Bucket='creafyn', Key='DescripcionEmpresas_BOT.xlsx')
    file_stream = BytesIO(response['Body'].read())
    df = pd.read_excel(file_stream, engine='openpyxl')

    df_seleccionado = df.iloc[:, [1, 3]]
    df_seleccionado.columns = ['RFC', 'DESCRIPCION']
    descripcion_row = df_seleccionado[df_seleccionado['RFC'] == rfc_key]

    if descripcion_row.empty:
        descripcion_text = "No se encontró descripción de la empresa."
        st.warning(descripcion_text)
    else:
        descripcion_text = descripcion_row.iloc[0]['DESCRIPCION']
        st.write("**Descripción de la empresa:**")
        st.info(descripcion_text)

    prompt = f"""
    Considerando que la empresa pertenece a la industria de: {descripcion_text}
    
    Analiza el panorama económico actual con base en los datos macroeconómicos más recientes de México
    y dime cómo pudiera afectar el panorama macroeconómico a las ventas de la empresa.

    Además, mencióname a qué variables macroeconómicas es más sensible la empresa y por qué.

    Evalúa también la situación económica actual/esperada en su región y a nivel nacional,
    y explica cómo factores como inflación, tasas de interés, tipo de cambio u otros pueden impactar sus ventas.

    Proporciona ejemplos específicos y análisis por industria.

    Utiliza fuentes confiables como INEGI, Banxico, Data México, Statista, McKinsey, PwC, Deloitte, KPMG, Bain, EY, SEC, Banco Mundial, FMI, OCDE, Concamin, Canacintra, AMIA, ABM, BBVA Research, Santander, El Economista, Expansión, Forbes, México Industry, Secretaria de Economía, SEC:Edgar, IMF, etc.
    Para este análisis considera el año 2025, febrero 2025
    """

    if st.button("Generar Análisis"):
        client = openai.OpenAI(api_key=st.secrets["openai_api_key"])
        response = client.chat.completions.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": "Eres un economista experto en análisis macroeconómico y riesgos financieros."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7
        )

        st.subheader("Análisis Económico:")
        st.markdown(response.choices[0].message.content)
